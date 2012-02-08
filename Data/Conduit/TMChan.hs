-- | Contains a simple source and sink for linking together conduits in
--   in different threads. Usage is so easy, it's best explained with an
--   example:
--
--   We first create a channel for communication...
--   > do chan <- atomically $ newTMChan
--
--   Then we fork a new thread loading a wackton of pictures into memory. The
--   data (pictures, in this case) will be streamed down the channel to whatever
--   is on the other side.
--   >    _ <- forkIO . runResourceT $ loadTextures lotsOfPictures $$ sinkTMChan chan
--
--   Finally, we connect something to the other end of the channel. In this
--   case, we connect a sink which uploads the textures one by one to the
--   graphics card.
--   >    runResourceT $ sourceTMChan chan $$ Conduit.mapM_ (liftIO . uploadToGraphicsCard)
--
--   By running the two tasks in parallel, we no longer have to wait for one
--   texture to upload to the graphics card before reading the next one from
--   disk. This avoids the common switching of bottlenecks (such as between the
--   disk and graphics memory) that most loading processes seem to love.
--
--   We re-export Control.Concurrent.STM.TMChan module for convenience.
module Data.Conduit.TMChan ( module Control.Concurrent.STM.TMChan
                           , sourceTMChan
                           , sinkTMChan
                           ) where

import Control.Monad.IO.Class ( liftIO )
import Control.Concurrent.STM ( atomically )
import Control.Concurrent.STM.TMChan

import Data.Conduit

-- | A simple wrapper around a TMChan. As data is pushed into the channel, the
--   source will read it and pass it down the conduit pipeline. When the
--   channel is closed, the source will close also.
sourceTMChan :: TMChan a -> Source IO a
sourceTMChan ch = src
    where
        src = Source pull close
        pull = do a <- liftIO . atomically $ readTMChan ch
                  case a of
                    Just x -> return $ Open src x
                    Nothing -> return $ Closed
        close = return () -- close is done by the sink.
{-# INLINE sourceTMChan #-}

-- | A simple wrapper around a TMChan. As data is pushed into this sink, it
--   will magically begin to appear in the channel. When the sink is closed,
--   the channel will close too.
sinkTMChan :: TMChan a -> Sink a IO ()
sinkTMChan ch = sink
    where
        sink = SinkData push close
        push input = do liftIO . atomically $ writeTMChan ch input
                        return $ Processing push close
        close = liftIO . atomically $ closeTMChan ch
{-# INLINE sinkTMChan #-}
