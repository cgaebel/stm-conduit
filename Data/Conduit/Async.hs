{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}

-- | * Introduction
--
--   Contain combinators for concurrently joining conduits, such that
--   the producing side may continue to produce (up to the queue size)
--   as the consumer is concurrently consuming.
module Data.Conduit.Async ( module Data.Conduit.Async.Composition
                          , gatherFrom
                          , drainTo
                          ) where

import           Control.Applicative
import           Control.Concurrent.Async.Lifted
import           Control.Concurrent.STM
import           Control.Exception.Lifted
import           Control.Monad.IO.Class
import           Control.Monad.Loops
import           Control.Monad.Trans.Class
import           Control.Monad.Trans.Control
import           Data.Conduit

import           Data.Conduit.Async.Composition

-- | Gather output values asynchronously from an action in the base monad and
--   then yield them downstream.  This provides a means of working around the
--   restriction that 'ConduitM' cannot be an instance of 'MonadBaseControl'
--   in order to, for example, yield values from within a Haskell callback
--   function called from a C library.
gatherFrom :: (MonadIO m, MonadBaseControl IO m)
           => Int                -- ^ Size of the queue to create
           -> (TBQueue o -> m ()) -- ^ Action that generates output values
           -> Producer m o
gatherFrom size scatter = do
    chan   <- liftIO $ newTBQueueIO size
    worker <- lift $ async (scatter chan)
    lift . restoreM =<< gather worker chan
  where
    gather worker chan = do
        (xs, mres) <- liftIO $ atomically $ do
            xs <- whileM (not <$> isEmptyTBQueue chan) (readTBQueue chan)
            (xs,) <$> pollSTM worker
        Prelude.mapM_ yield xs
        case mres of
            Just (Left e)  -> liftIO $ throwIO (e :: SomeException)
            Just (Right r) -> return r
            Nothing        -> gather worker chan

-- | Drain input values into an asynchronous action in the base monad via a
--   bounded 'TBQueue'.  This is effectively the dual of 'gatherFrom'.
drainTo :: (MonadIO m, MonadBaseControl IO m)
        => Int                        -- ^ Size of the queue to create
        -> (TBQueue (Maybe i) -> m r)  -- ^ Action to consume input values
        -> Consumer i m r
drainTo size gather = do
    chan   <- liftIO $ newTBQueueIO size
    worker <- lift $ async (gather chan)
    lift . restoreM =<< scatter worker chan
  where
    scatter worker chan = do
        mval <- await
        (mx, action) <- liftIO $ atomically $ do
            mres <- pollSTM worker
            case mres of
                Just (Left e)  ->
                    return (Nothing, liftIO $ throwIO (e :: SomeException))
                Just (Right r) ->
                    return (Just r, return ())
                Nothing        -> do
                    writeTBQueue chan mval
                    return (Nothing, return ())
        action
        case mx of
            Just x  -> return x
            Nothing -> scatter worker chan
