{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE KindSignatures #-}

-- | * Introduction
--
--   Contains a simple source and sink for linking together conduits in
--   in different threads. Usage is so easy, it's best explained with an
--   example:
--
--   We first create a channel for communication...
--
--   > do chan <- atomically $ newTBMChan 16
--
--   Then we fork a new thread loading a wackton of pictures into memory. The
--   data (pictures, in this case) will be streamed down the channel to whatever
--   is on the other side.
--
--   >    _ <- forkIO . runResourceT $ loadTextures lotsOfPictures $$ sinkTBMChan chan
--
--   Finally, we connect something to the other end of the channel. In this
--   case, we connect a sink which uploads the textures one by one to the
--   graphics card.
--
--   >    runResourceT $ sourceTBMChan chan $$ Conduit.mapM_ (liftIO . uploadToGraphicsCard)
--
--   By running the two tasks in parallel, we no longer have to wait for one
--   texture to upload to the graphics card before reading the next one from
--   disk. This avoids the common switching of bottlenecks (such as between the
--   disk and graphics memory) that most loading processes seem to love.
--
--   Control.Concurrent.STM.TMChan and Control.Concurrent.STM.TBMChan are
--   re-exported for convenience.
--
--   * Caveats
--
--   It is recommended to use TBMChan as much as possible, and generally avoid
--   TMChan usage. TMChans are unbounded, and if used, the conduit pipeline
--   will no longer use a bounded amount of space. They will essentially leak
--   memory if the writer is faster than the reader.
--
--   Therefore, use bounded channels as much as possible, preferably with a
--   high bound so it will be hit infrequently.
module Data.Conduit.TMChan ( -- * Bounded Channel Connectors
                             module Control.Concurrent.STM.TBMChan
                           , sourceTBMChan
                           , sinkTBMChan
                           -- * Unbounded Channel Connectors
                           , module Control.Concurrent.STM.TMChan
                           , sourceTMChan
                           , sinkTMChan
                           -- * Parallel Combinators
                           , (>=<)
                           , mergeSources
                           , (<=>)
                           , mergeConduits
                           ) where

import Control.Monad
import Control.Monad.IO.Class ( liftIO, MonadIO )
import Control.Monad.Trans.Class
import Control.Monad.Trans.Resource
import Control.Concurrent (killThread, forkIOWithUnmask)
import Control.Concurrent.STM hiding (atomically)
import Control.Concurrent.STM.TBMChan
import Control.Concurrent.STM.TMChan

import Data.Conduit
import Data.Foldable
import qualified Data.Conduit.List as CL
import UnliftIO as Lifted

-- | Convert channel into the source.
--
-- *N.B* Since version 4.0 this function does not close the
-- channel if downstream is closed.
chanSource
    :: MonadIO m
    => chan                     -- ^ The channel.
    -> (chan -> STM (Maybe a))  -- ^ The 'read' function.
    -> ConduitT z a m ()
chanSource ch reader =
    loop
  where
    loop = do
        a <- liftSTM $ reader ch
        case a of
            Just x  -> yield x >> loop
            Nothing -> return ()
{-# INLINE chanSource #-}

-- | Convert channel into the consumer.
--
-- *N.B*
chanSink
    :: MonadIO m
    => chan                     -- ^ The channel.
    -> (chan -> a -> STM ())    -- ^ The 'write' function.
    -> ConduitT a z m ()
chanSink ch writer = CL.mapM_ $ liftIO . atomically . writer ch
{-# INLINE chanSink #-}

-- | A simple wrapper around a TBMChan. As data is pushed into the channel, the
--   source will read it and pass it down the conduit pipeline. When the
--   channel is closed, the source will close also.
--
--   If the channel fills up, the pipeline will stall until values are read.
sourceTBMChan :: MonadIO m => TBMChan a -> ConduitT () a m ()
sourceTBMChan ch = chanSource ch readTBMChan
{-# INLINE sourceTBMChan #-}

-- | A simple wrapper around a TMChan. As data is pushed into the channel, the
--   source will read it and pass it down the conduit pipeline. When the
--   channel is closed, the source will close also.
sourceTMChan :: MonadIO m => TMChan a -> ConduitT () a m ()
sourceTMChan ch = chanSource ch readTMChan
{-# INLINE sourceTMChan #-}

-- | A simple wrapper around a TBMChan. As data is pushed into the sink, it
--   will magically begin to appear in the channel. If the channel is full,
--   the sink will block until space frees up.
sinkTBMChan :: MonadIO m
            => TBMChan a
            -> ConduitT a z m ()
sinkTBMChan ch = chanSink ch writeTBMChan
{-# INLINE sinkTBMChan #-}

-- | A simple wrapper around a TMChan. As data is pushed into this sink, it
--   will magically begin to appear in the channel.
sinkTMChan :: MonadIO m
           => TMChan a
           -> ConduitT a z m ()
sinkTMChan ch = chanSink ch writeTMChan
{-# INLINE sinkTMChan #-}

infixl 5 >=<

-- | Modifies a TVar, returning its new value.
modifyTVar'' :: TVar a -> (a -> a) -> STM a
modifyTVar'' tv f = do
  !x <- f <$> readTVar tv
  writeTVar tv x
  return x

liftSTM :: forall (m :: * -> *) a. MonadIO m => STM a -> m a
liftSTM = liftIO . atomically

-- | Combines two sources with an unbounded channel, creating a new source
--   which pulls data from a mix of the two sources: whichever produces first.
--
--   The order of the new source's data is undefined, but it will be some
--   combination of the two given sources.
(>=<) :: (MonadResource mi, MonadIO mo, MonadUnliftIO mi)
      => ConduitT () a mi ()
      -> ConduitT () a mi ()
      -> mo (ConduitT () a mi ())
sa >=< sb = mergeSources [ sa, sb ] 16
{-# INLINE (>=<) #-}

decRefcount :: TVar Int -> TBMChan a ->  STM ()
decRefcount tv chan = do
  n <- modifyTVar'' tv (subtract 1)
  when (n == 0) $
    closeTBMChan chan

-- | Merges a list of sources, putting them all into a bounded channel, and
--   returns a source which can be pulled from to pull from all the given
--   sources in a first-come-first-serve basis.
--
--   The order of the new source's data is undefined, but it will be some
--   combination of the given sources. The monad of the resultant source
--   (@mo@) is independent of the monads of the input sources (@mi@).
--
--   @since 3.0
--   All spawned threads will be removed when source is closed or upon an
--   exit from 'ResourceT' region. This means that result can only be used
--   within a 'runResourceT' scope.
--
--   @before 3.0
--   Spawned threads are not guaranteed to be closed. This may happen if
--   Source was closed before all it's input were closed.
mergeSources :: (MonadResource mi, MonadIO mo, MonadUnliftIO mi)
             => [ConduitT () a mi ()] -- ^ The sources to merge.
             -> Int -- ^ The bound of the intermediate channel.
             -> mo (ConduitT () a mi ())
mergeSources sx bound = do
     return $ do 
       (chkey, c) <- allocate (liftSTM $ newTBMChan bound)
                              (liftSTM . closeTBMChan)
       refcount <- liftSTM . newTVar $ length sx
       st <- lift $ askUnliftIO
       regs <- forM sx $ \s ->
              register . killThread =<<
                (liftIO $ forkIOWithUnmask $ \unmask ->
                   (unmask $ unliftIO st $
                      runConduit $ s .| chanSink c writeTBMChan)
                    `Lifted.finally` (liftSTM $ decRefcount refcount c))
       chanSource c readTBMChan
       release chkey
       traverse_ release regs

-- | Combines two conduits with unbounded channels, creating a new conduit
--   which pulls data from a mix of the two: whichever produces first.
--
--   The order of the new conduit's output is undefined, but it will be some
--   combination of the two given conduits.
(<=>) :: (MonadThrow mi, MonadIO mo, MonadUnliftIO mi)
      => ConduitT i i (ResourceT mi) ()
      -> ConduitT i i (ResourceT mi) ()
      -> ResourceT mi (ConduitT i i mo ())
sa <=> sb = mergeConduits [ sa, sb ] 16
{-# INLINE (<=>) #-}

-- | Provide an input across several conduits, putting them all into a bounded
--   channel. Returns a conduit which can be pulled from to pull from all the
--   given conduits in a first-come-first-serve basis.
--
--   The order of the new conduits's outputs is undefined, but it will be some
--   combination of the given conduits. The monad of the resultant conduit
--   (@mo@) is independent of the monads of the input conduits (@mi@).
--
-- @since 3.0
--   Closes all worker processes when resulting conduit is closed or when execution
--   leaves ResourceT context. This means that conduit is only valid inside
--   'runResouceT' scope.
--
-- @before 3.0
--   Spawned threads are not guaranteed to be closed, This may happen if threads
--   Conduit was closed before all threads have finished execution.
{-# DEPRECATED mergeConduits "This method will dissapear in the next version." #-}
mergeConduits :: (MonadIO mo, MonadUnliftIO mi)
              => [ConduitT i o (ResourceT mi) ()] -- ^ The conduits to merge.
              -> Int -- ^ The bound for the channels.
              -> ResourceT mi (ConduitT i o mo ())
mergeConduits conduits bound = do
        let len = length conduits
        refcount <- liftSTM $ newTVar len
        (iregs, iChannels) <- fmap unzip 
             $ replicateM len $ allocate (liftSTM $ newTBMChan bound)
                                          (liftSTM . closeTBMChan)
        (oreg, oChannel) <- allocate (liftSTM $ newTBMChan bound)
                                     (liftSTM . closeTBMChan)
        regs <- forM (zip iChannels conduits)
            $ \(iChannel, conduit) -> Lifted.mask_ $
              register . killThread <=< resourceForkIO
                $ (runConduit $ sourceTBMChan iChannel
                             .| conduit
                             .| chanSink oChannel writeTBMChan)
                     `finally` 
                        (liftIO $ atomically $ decRefcount refcount oChannel >> closeTBMChan iChannel)
        treg <- register $ do 
          traverse_ release regs
          traverse_ release iregs
          release oreg
        return
            $  do chanSink iChannels writeTBMChans
                  -- release internal channels effiniently closing input channels
                  traverse_ release iregs
                  chanSource oChannel readTBMChan
                  -- release internals everything
                  release treg
  where
    writeTBMChans channels a = forM_ channels $ \c -> writeTBMChan c a
