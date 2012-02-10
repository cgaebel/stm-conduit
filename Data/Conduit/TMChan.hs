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
module Data.Conduit.TMChan ( module Control.Concurrent.STM.TBMChan
                           , sourceTBMChan
                           , sinkTBMChan
                           , module Control.Concurrent.STM.TMChan
                           , sourceTMChan
                           , sinkTMChan
                           ) where

import Control.Monad.IO.Class ( liftIO )
import Control.Concurrent.STM
import Control.Concurrent.STM.TBMChan
import Control.Concurrent.STM.TMChan

import Data.Conduit

chanSource 
    :: ResourceIO m 
    => chan                     -- ^ The channel.
    -> (chan -> STM (Maybe a))  -- ^ The 'read' function.
    -> (chan -> STM ())         -- ^ The 'close' function.
    -> Source m a
chanSource ch reader closer = src
    where
        src = Source pull close
        pull = do a <- liftIO . atomically $ reader ch
                  case a of
                    Just x  -> return $ Open src x
                    Nothing -> return Closed
        close = liftIO . atomically $ closer ch
{-# INLINE chanSource #-}

chanSink 
    :: ResourceIO m
    => chan                     -- ^ The channel.
    -> (chan -> a -> STM ())    -- ^ The 'write' function.
    -> (chan -> STM ())         -- ^ The 'close' function.
    -> Sink a m ()
chanSink ch writer closer = sink
    where
        sink = SinkData push close
        push input = do liftIO . atomically $ writer ch input
                        return $ Processing push close
        close = liftIO . atomically $ closer ch
{-# INLINE chanSink #-}

-- | A simple wrapper around a TBMChan. As data is pushed into the channel, the
--   source will read it and pass it down the conduit pipeline. When the
--   channel is closed, the source will close also.
--
--   If the channel fills up, the pipeline will stall until values are read.
sourceTBMChan :: ResourceIO m => TBMChan a -> Source m a
sourceTBMChan ch = chanSource ch readTBMChan closeTBMChan
{-# INLINE sourceTBMChan #-}

-- | A simple wrapper around a TMChan. As data is pushed into the channel, the
--   source will read it and pass it down the conduit pipeline. When the
--   channel is closed, the source will close also.
sourceTMChan :: ResourceIO m => TMChan a -> Source m a
sourceTMChan ch = chanSource ch readTMChan closeTMChan
{-# INLINE sourceTMChan #-}

-- | A simple wrapper around a TBMChan. As data is pushed into the sink, it
--   will magically begin to appear in the channel. If the channel is full,
--   the sink will block until space frees up. When the sink is closed, the
--   channel will close too.
sinkTBMChan :: ResourceIO m => TBMChan a -> Sink a m ()
sinkTBMChan ch = chanSink ch writeTBMChan closeTBMChan
{-# INLINE sinkTBMChan #-}

-- | A simple wrapper around a TMChan. As data is pushed into this sink, it
--   will magically begin to appear in the channel. When the sink is closed,
--   the channel will close too.
sinkTMChan :: ResourceIO m => TMChan a -> Sink a m ()
sinkTMChan ch = chanSink ch writeTMChan closeTMChan
{-# INLINE sinkTMChan #-}
