{-# LANGUAGE NoMonomorphismRestriction, FlexibleContexts, RankNTypes,KindSignatures #-}
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

import Control.Applicative
import Control.Monad
import Control.Monad.IO.Class ( liftIO, MonadIO )
import Control.Monad.Trans.Class
import Control.Monad.Trans.Resource
import Control.Concurrent.STM
import Control.Concurrent.STM.TBMChan
import Control.Concurrent.STM.TMChan

import Data.Conduit
import qualified Data.Conduit.List as CL

chanSource
    :: MonadIO m
    => chan                     -- ^ The channel.
    -> (chan -> STM (Maybe a))  -- ^ The 'read' function.
    -> (chan -> STM ())         -- ^ The 'close' function.
    -> Source m a
chanSource ch reader closer =
    loop
  where
    loop = do
        a <- liftSTM $ reader ch
        case a of
            Just x  -> yieldOr x close >> loop
            Nothing -> return ()
    close = liftSTM $ closer ch
{-# INLINE chanSource #-}

chanSink
    :: MonadIO m
    => chan                     -- ^ The channel.
    -> (chan -> a -> STM ())    -- ^ The 'write' function.
    -> (chan -> STM ())         -- ^ The 'close' function.
    -> Sink a m ()
chanSink ch writer closer = do
    CL.mapM_ $ liftIO . atomically . writer ch
    liftSTM $ closer ch
{-# INLINE chanSink #-}

-- | A simple wrapper around a TBMChan. As data is pushed into the channel, the
--   source will read it and pass it down the conduit pipeline. When the
--   channel is closed, the source will close also.
--
--   If the channel fills up, the pipeline will stall until values are read.
sourceTBMChan :: MonadIO m => TBMChan a -> Source m a
sourceTBMChan ch = chanSource ch readTBMChan closeTBMChan
{-# INLINE sourceTBMChan #-}

-- | A simple wrapper around a TMChan. As data is pushed into the channel, the
--   source will read it and pass it down the conduit pipeline. When the
--   channel is closed, the source will close also.
sourceTMChan :: MonadIO m => TMChan a -> Source m a
sourceTMChan ch = chanSource ch readTMChan closeTMChan
{-# INLINE sourceTMChan #-}

-- | A simple wrapper around a TBMChan. As data is pushed into the sink, it
--   will magically begin to appear in the channel. If the channel is full,
--   the sink will block until space frees up.
sinkTBMChan :: MonadIO m
            => TBMChan a
            -> Bool -- ^ Should the channel be closed when the sink is closed?
            -> Sink a m ()
sinkTBMChan ch close = chanSink ch writeTBMChan (when close . closeTBMChan)
{-# INLINE sinkTBMChan #-}

-- | A simple wrapper around a TMChan. As data is pushed into this sink, it
--   will magically begin to appear in the channel.
sinkTMChan :: MonadIO m
           => TMChan a
           -> Bool -- ^ Should the channel be closed when the sink is closed?
           -> Sink a m ()
sinkTMChan ch close = chanSink ch writeTMChan (when close . closeTMChan)
{-# INLINE sinkTMChan #-}

infixl 5 >=<

-- | Modifies a TVar, returning its new value.
modifyTVar'' :: TVar a -> (a -> a) -> STM a
modifyTVar'' tv f = do x <- f <$> readTVar tv
                       writeTVar tv x
                       return x

liftSTM :: forall (m :: * -> *) a. MonadIO m => STM a -> m a
liftSTM = liftIO . atomically

-- | Combines two sources with an unbounded channel, creating a new source
--   which pulls data from a mix of the two sources: whichever produces first.
--
--   The order of the new source's data is undefined, but it will be some
--   combination of the two given sources.
(>=<) :: (MonadResource mi, MonadIO mo, MonadBaseControl IO mi)
      => Source mi a
      -> Source mi a
      -> mi (Source mo a)
sa >=< sb = mergeSources [ sa, sb ] 16
{-# INLINE (>=<) #-}

decRefcount :: TVar Int -> TBMChan a ->  STM ()
decRefcount tv chan = do n <- modifyTVar'' tv (subtract 1)
                         when (n == 0) $
                            closeTBMChan chan

-- | Merges a list of sources, putting them all into a bounded channel, and
--   returns a source which can be pulled from to pull from all the given
--   sources in a first-come-first-serve basis.
--
--   The order of the new source's data is undefined, but it will be some
--   combination of the given sources. The monad of the resultant source
--   (@mo@) is independent of the monads of the input sources (@mi@).
mergeSources :: (MonadResource mi, MonadIO mo, MonadBaseControl IO mi)
             => [Source mi a] -- ^ The sources to merge.
             -> Int -- ^ The bound of the intermediate channel.
             -> mi (Source mo a)
mergeSources sx bound = do c <- liftSTM $ newTBMChan bound
                           refcount <- liftSTM . newTVar $ length sx
                           mapM_ (\s -> runResourceT $ resourceForkIO $ s $$ chanSink c writeTBMChan $ decRefcount refcount) (map (transPipe lift) sx)
                           return $ sourceTBMChan c

-- | Combines two conduits with unbounded channels, creating a new conduit
--   which pulls data from a mix of the two: whichever produces first.
--
--   The order of the new conduit's output is undefined, but it will be some
--   combination of the two given conduits.
(<=>) :: (MonadIO mi, MonadIO mo, MonadBaseControl IO mi)
      => Show i
      => Conduit i (ResourceT mi) i
      -> Conduit i (ResourceT mi) i
      -> ResourceT mi (Conduit i mo i)
sa <=> sb = mergeConduits [ sa, sb ] 16
{-# INLINE (<=>) #-}

-- | Provide an input across several conduits, putting them all into a bounded
--   channel. Returns a conduit which can be pulled from to pull from all the
--   given conduits in a first-come-first-serve basis.
--
--   The order of the new conduits's outputs is undefined, but it will be some
--   combination of the given conduits. The monad of the resultant conduit
--   (@mo@) is independent of the monads of the input conduits (@mi@).
mergeConduits :: (MonadIO mi, MonadIO mo, MonadBaseControl IO mi)
              => [Conduit i (ResourceT mi) o] -- ^ The conduits to merge.
              -> Int -- ^ The bound for the channels.
              -> ResourceT mi (Conduit i mo o)
mergeConduits conduits bound = do
        let len = length conduits
        refcount <- liftSTM $ newTVar len
        iChannels <- replicateM len $ liftSTM $ newTBMChan bound
        oChannel <- liftSTM $ newTBMChan bound
        forM_ (zip iChannels conduits)
            $ \(iChannel, conduit) -> resourceForkIO
                $  sourceTBMChan iChannel
                $$ conduit
                =$ chanSink oChannel (writeTBMChans . (:[])) (decRefcount refcount)
        return
            $  toConsumer (chanSink iChannels writeTBMChans (mapM_ closeTBMChan))
            >> toProducer (sourceTBMChan oChannel)
  where
    writeTBMChans channels a = forM_ channels $ \c -> writeTBMChan c a
