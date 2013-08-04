{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}

-- | * Introduction
--
--   Contains a combinator for concurrently joining a producer and a consumer,
--   such that the producer may continue to produce (up to the queue size) as
--   the consumer is concurrently consuming.
module Data.Conduit.Async where

import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Monad.IO.Class
import Control.Monad.Trans.Control
import Data.Conduit
import Data.Conduit.List
import Prelude hiding (mapM_)

-- | Concurrently join the producer and consumer, using a bounded queue of the
--   given size.  The producer will block when the queue is full, if it is
--   producing faster than the consumers is taking from it.  Likewise, if the
--   consumer races ahead, it will block until more input is available.
--
--   Exceptions are properly managed and propagated between the two sides, so
--   the net effect should be equivalent to not using buffer at all, save for
--   the concurrent interleaving of effects.
buffer :: (MonadBaseControl IO m, MonadIO m)
       => Int -> Producer m a -> Consumer a m b -> m b
buffer size input output = do
    chan <- liftIO $ newTBQueueIO size
    control $ \runInIO ->
        withAsync (runInIO $ sender chan) $ \input' ->
            withAsync (runInIO $ recv chan $$ output) $ \output' -> do
                link2 input' output'
                wait output'
  where
    send chan = liftIO . atomically . writeTBQueue chan

    sender chan = do
        input $$ mapM_ (send chan . Just)
        send chan Nothing

    recv chan = do
        mx <- liftIO $ atomically $ readTBQueue chan
        case mx of
            Nothing -> return ()
            Just x  -> yield x >> recv chan

-- | An operator form of 'buffer'.  In general you should be able to replace
--   any use of 'Data.Conduit.$$' with '$$&' and suddenly reap the benefit of
--   concurrency, if your conduits were spending time waiting on each other.
($$&) :: (MonadIO m, MonadBaseControl IO m)
      => Producer m a -> Consumer a m b -> m b
($$&) = buffer 64
