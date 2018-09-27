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
  , entangledPair
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
import qualified Data.Conduit.List as CL

-- | A simple wrapper around a "TQueue". As data is pushed into the queue, the
--   source will read it and pass it down the conduit pipeline.
sourceTQueue :: MonadIO m => TQueue a -> ConduitT z a m ()
sourceTQueue q = forever $ liftSTM (readTQueue q) >>= yield

-- | A simple wrapper around a "TQueue". As data is pushed into this sink, it
--   will magically begin to appear in the queue.
sinkTQueue :: MonadIO m => TQueue a -> ConduitT a z m ()
sinkTQueue q = CL.mapM_ (liftSTM . writeTQueue q)

-- | A simple wrapper around a "TBQueue". As data is pushed into the queue, the
--   source will read it and pass it down the conduit pipeline.
sourceTBQueue :: MonadIO m => TBQueue a -> ConduitT z a m ()
sourceTBQueue q = forever $ liftSTM (readTBQueue q) >>= yield

-- | A simple wrapper around a "TBQueue". As data is pushed into this sink, it
--   will magically begin to appear in the queue.
sinkTBQueue :: MonadIO m => TBQueue a -> ConduitT a z m ()
sinkTBQueue q = CL.mapM_ (liftSTM . writeTBQueue q)

-- | A convenience wrapper for creating a source and sink TBQueue of the given
--   size at once, without exposing the underlying queue.
--
-- Returns release key that can be used for premature close of the communication
-- channel, otherwise channel will be closed when the ResourceT scope will be closed.
entangledPair :: MonadIO m => Int -> m (ConduitT z a m (), ConduitT a l m ())
entangledPair size = liftM (liftM2 (,) sourceTBQueue sinkTBQueue) $
  liftIO $ atomically $ newTBQueue (fromIntegral size)

-- | A simple wrapper around a "TMQueue". As data is pushed into the queue, the
--   source will read it and pass it down the conduit pipeline. When the
--   queue is closed, the source will close also.
sourceTMQueue :: MonadIO m => TMQueue a -> ConduitT z a m ()
sourceTMQueue q =
    loop
  where
    loop = do
        mx <- liftSTM $ readTMQueue q
        case mx of
            Nothing -> return ()
            Just x -> yield x >> loop

-- | A simple wrapper around a "TMQueue". As data is pushed into this sink, it
--   will magically begin to appear in the queue.
sinkTMQueue :: MonadIO m
            => TMQueue a
            -> ConduitT a z m ()
sinkTMQueue q = CL.mapM_ (liftSTM . writeTMQueue q)

-- | A simple wrapper around a "TBMQueue". As data is pushed into the queue, the
--   source will read it and pass it down the conduit pipeline. When the
--   queue is closed, the source will close also.
sourceTBMQueue :: MonadIO m => TBMQueue a -> ConduitT z a m ()
sourceTBMQueue q =
    loop
  where
    loop = do
        mx <- liftSTM $ readTBMQueue q
        case mx of
            Nothing -> return ()
            Just x -> yield x >> loop

-- | A simple wrapper around a "TBMQueue". As data is pushed into this sink, it
--   will magically begin to appear in the queue.
sinkTBMQueue :: MonadIO m
             => TBMQueue a
             -> ConduitT a z m ()
sinkTBMQueue q = CL.mapM_ (liftSTM . writeTBMQueue q)


liftSTM :: forall (m :: * -> *) a. MonadIO m => STM a -> m a
liftSTM = liftIO . atomically

-- $infinite
-- It's impossible to close infinite queues but they work slightly faster,
-- so it's reasonable to use them inside infinite computations for
-- performance reasons.
