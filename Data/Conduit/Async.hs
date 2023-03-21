{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

-- | * Introduction
--
--   Contain combinators for concurrently joining conduits, such that
--   the producing side may continue to produce (up to the queue size)
--   as the consumer is concurrently consuming.
module Data.Conduit.Async ( module Data.Conduit.Async.Composition
                          , gatherFrom
                          , drainTo
                          , Data.Conduit.Async.mapConcurrently
                          ) where

import Conduit (MonadResource)
import Control.Concurrent.STM.TBMChan
import Control.Monad.IO.Class
import Control.Monad.Loops
import Control.Monad.Trans.Class
import Data.Conduit
import Data.Conduit.Async.Composition
import Data.Foldable
import qualified Data.IntMap as IM
import UnliftIO
import Control.Monad.STM (retry)
import Control.Monad (replicateM, mapM_)

-- | Gather output values asynchronously from an action in the base monad and
--   then yield them downstream.  This provides a means of working around the
--   restriction that 'ConduitM' cannot be an instance of 'MonadBaseControl'
--   in order to, for example, yield values from within a Haskell callback
--   function called from a C library.
gatherFrom :: (MonadIO m, MonadUnliftIO m)
           => Int                -- ^ Size of the queue to create
           -> (TBQueue o -> m ()) -- ^ Action that generates output values
           -> ConduitT () o m ()
gatherFrom size scatter = do
    chan   <- liftIO $ newTBQueueIO (fromIntegral size)
    worker <- lift $ async (scatter chan)
    gather worker chan
  where
    gather worker chan = do
        (xs, mres) <- liftIO $ atomically $ do
            xs <- whileM (not <$> isEmptyTBQueue chan) (readTBQueue chan)
            (xs,) <$> pollSTM worker
        traverse_ yield xs
        case mres of
            Just (Left e)  -> liftIO $ throwIO (e :: SomeException)
            Just (Right r) -> return r
            Nothing        -> gather worker chan

-- | Drain input values into an asynchronous action in the base monad via a
--   bounded 'TBQueue'.  This is effectively the dual of 'gatherFrom'.
drainTo :: (MonadIO m, MonadUnliftIO m)
        => Int                        -- ^ Size of the queue to create
        -> (TBQueue (Maybe i) -> m r)  -- ^ Action to consume input values
        -> ConduitT i Void m r
drainTo size gather = do
    chan   <- liftIO $ newTBQueueIO (fromIntegral size)
    worker <- lift $ async (gather chan)
    scatter worker chan
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

-- | Concurrently process input by spawning worker threads.
mapConcurrently ::
  forall i o m.
  (MonadResource m, MonadUnliftIO m) =>
  -- | Number of workers to spawn
  Int ->
  -- | Size of the input buffer
  Int ->
  -- | Size of the output buffer
  Int ->
  -- | Action to perform
  (i -> m o) ->
  ConduitT i o m ()
mapConcurrently workers inBufferSize outBufferSize f = do
  inBuffer <- liftIO $ newTBMChanIO inBufferSize
  outBuffer <- liftIO $ newTVarIO IM.empty
  runInIO <- lift askRunInIO
  bracketP
    ( replicateM
        workers
        ( do thread <- async (runInIO (workerLoop inBuffer outBuffer))
             link thread
             return thread
        )
    )
    (mapM_ wait)
    $ \_ -> go (-1) 0 inBuffer outBuffer
  where
    go :: Int -> Int -> TBMChan (i, Int) -> TVar (IM.IntMap o) -> ConduitT i o m ()
    go maxIndexIn nextIndexOut inBuffer outBuffer = do
      nextAction :: ConduitT i o m () <-
        atomically $ do
          inBufferClosed <- isClosedTBMChan inBuffer
          if inBufferClosed && nextIndexOut > maxIndexIn
            then return (return ())
            else do
              outBufferContent <- readTVar outBuffer
              inBufferFull <- isFullTBMChan inBuffer
              if not inBufferFull && not inBufferClosed
                -- First try to saturate the input buffer
                then return $ do
                  mNext <- await
                  case mNext of
                    Nothing -> do
                      atomically $ closeTBMChan inBuffer
                      go maxIndexIn nextIndexOut inBuffer outBuffer
                    Just next -> do
                      let idx = maxIndexIn + 1
                      atomically $ writeTBMChan inBuffer (next, idx)
                      go idx nextIndexOut inBuffer outBuffer
                -- Only when the input buffer has no capacity check whether there is output available.
                else case IM.minViewWithKey outBufferContent of
                  Just ((lowestIdx, res), outBufferRest) | lowestIdx == nextIndexOut -> do
                    writeTVar outBuffer outBufferRest
                    return $ do
                      yield res
                      go maxIndexIn (nextIndexOut + 1) inBuffer outBuffer
                  _ -> retry

      nextAction

    workerLoop inBuffer outBuffer = do
      mNext <- atomically $ readTBMChan inBuffer
      case mNext of
        Nothing -> return ()
        Just (req, idx) -> do
          res <- f req
          atomically $ do
            outBufferContent <- readTVar outBuffer
            case IM.minViewWithKey outBufferContent of
              Just ((lowestIdx, _), _)
                | lowestIdx < idx - outBufferSize -> retry
              _ -> writeTVar outBuffer (IM.insert idx res outBufferContent)
          workerLoop inBuffer outBuffer