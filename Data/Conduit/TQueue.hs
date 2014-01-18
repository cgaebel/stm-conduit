{-# LANGUAGE Rank2Types, KindSignatures #-}
-- | Contains a simple source and sink linking together conduits in
--   different threads. For extended examples of usage and bottlenecks
--   see 'Data.Conduit.TMChan'.
--
--   TQueue is an amoritized FIFO queue behaves like TChan, with two
--   important differences:
--
--     * it's faster (but amortized thus the cost of individual operations
--     may vary a lot)
--
--     * it doesn't provide equivalent of the dupTChan and cloneTChan
--     operations
--
--
--   Here is short description of data structures:
--     
--     * TQueue   - unbounded infinite queue
--
--     * TBQueue  - bounded infinite queue
--
--     * TMQueue  - unbounded finite (closable) queue
--
--     * TBMQueue - bounded finite (closable) queue
--
-- Caveats
-- 
--   Infinite operations means that source doesn't know when stream is
--   ended so one need to use other methods of finishing stream like
--   sending an exception or finish conduit in downstream.
--

module Data.Conduit.TQueue
  ( -- * Connectors
    -- ** Infinite queues
    -- $inifinite
    -- *** TQueue connectors
    sourceTQueue
  , sinkTQueue
    -- *** TBQueue connectors
  , sourceTBQueue
  , sinkTBQueue
    -- ** Closable queues
    -- *** TMQueue connectors
  , sourceTMQueue
  , sinkTMQueue
    -- *** TBMQueue connectors
  , sourceTBMQueue
  , sinkTBMQueue
  , module Control.Concurrent.STM.TQueue
  ) where

import Control.Concurrent.STM
import Control.Concurrent.STM.TQueue
import Control.Concurrent.STM.TBMQueue
import Control.Concurrent.STM.TMQueue
import Control.Monad
import Control.Monad.IO.Class
import Data.Conduit
import Data.Conduit.Internal

-- | A simple wrapper around a "TQueue". As data is pushed into the queue, the
--   source will read it and pass it down the conduit pipeline.
sourceTQueue :: MonadIO m => TQueue a -> Source m a
sourceTQueue q = ConduitM src
  where src = PipeM pull
        pull = do x <- liftSTM $ readTQueue q
                  return $ HaveOutput src close x
        close = return ()

-- | A simple wrapper around a "TQueue". As data is pushed into this sink, it
--   will magically begin to appear in the queue.
sinkTQueue :: MonadIO m => TQueue a -> Sink a m ()
sinkTQueue q = ConduitM src
  where src        = sink
        sink       = NeedInput push close
        push input = PipeM ((liftSTM $ writeTQueue q input)
                            >> (return $ NeedInput push close))
        close _    = return ()

-- | A simple wrapper around a "TBQueue". As data is pushed into the queue, the
--   source will read it and pass it down the conduit pipeline.
sourceTBQueue :: MonadIO m => TBQueue a -> Source m a
sourceTBQueue q = ConduitM src
  where src = PipeM pull
        pull = do x <- liftSTM $ readTBQueue q
                  return $ HaveOutput src close x
        close = return ()

-- | A simple wrapper around a "TBQueue". As data is pushed into this sink, it
--   will magically begin to appear in the queue. Boolean argument is used
--   to specify if queue should be closed when the sink is closed.
sinkTBQueue :: MonadIO m => TBQueue a -> Sink a m ()
sinkTBQueue q = ConduitM src
  where src        = sink
        sink       = NeedInput push close
        push input = PipeM ((liftSTM $ writeTBQueue q input)
                            >> (return $ NeedInput push close))
        close _    = return ()

-- | A simple wrapper around a "TMQueue". As data is pushed into the queue, the
--   source will read it and pass it down the conduit pipeline. When the
--   queue is closed, the source will close also.
sourceTMQueue :: MonadIO m => TMQueue a -> Source m a
sourceTMQueue q = ConduitM src
  where src = PipeM pull
        pull = do mx <- liftSTM $ readTMQueue q
                  case mx of
                      Nothing -> return $ Done ()
                      Just x -> return $ HaveOutput src close x
        close = do liftSTM $ closeTMQueue q
                   return ()

-- | A simple wrapper around a "TMQueue". As data is pushed into this sink, it
--   will magically begin to appear in the queue.
sinkTMQueue :: MonadIO m
            => TMQueue a
            -> Bool -- ^ Should the queue be closed when the sink is closed?
            -> Sink a m ()
sinkTMQueue q shouldClose = ConduitM src
  where src = sink
        sink =  NeedInput push close
        push input = PipeM ((liftSTM $ writeTMQueue q input)
                            >> (return $ NeedInput push close))
        close _ = do when shouldClose (liftSTM $ closeTMQueue q)
                     return ()

-- | A simple wrapper around a "TBMQueue". As data is pushed into the queue, the
--   source will read it and pass it down the conduit pipeline. When the
--   queue is closed, the source will close also.
sourceTBMQueue :: MonadIO m => TBMQueue a -> Source m a
sourceTBMQueue q = ConduitM src
  where src = PipeM pull
        pull = do mx <- liftSTM $ readTBMQueue q
                  case mx of
                      Nothing -> return $ Done ()
                      Just x -> return $ HaveOutput src close x
        close = do liftSTM $ closeTBMQueue q
                   return ()

-- | A simple wrapper around a "TBMQueue". As data is pushed into this sink, it
--   will magically begin to appear in the queue.
sinkTBMQueue :: MonadIO m
             => TBMQueue a
             -> Bool -- ^ Should the queue be closed when the sink is closed?
             -> Sink a m ()
sinkTBMQueue q shouldClose = ConduitM src
  where src = sink
        sink =  NeedInput push close
        push input = PipeM ((liftSTM $ writeTBMQueue q input)
                            >> (return $ NeedInput push close))
        close _ = do when shouldClose (liftSTM $ closeTBMQueue q)
                     return ()


liftSTM :: forall (m :: * -> *) a. MonadIO m => STM a -> m a
liftSTM = liftIO . atomically

-- $infinite
-- It's impossible to close infinite queues but they work slightly faster,
-- so it's reasonable to use them inside infinite computations for
-- performance reasons.
