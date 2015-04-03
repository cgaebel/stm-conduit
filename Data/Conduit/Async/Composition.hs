{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Data.Conduit.Async.Composition ( CConduit
                                      , CFConduit
                                      , ($=&)
                                      , (=$&)
                                      , (=$=&)
                                      , ($$&)
                                      , buffer
                                      , buffer'
                                      , bufferToFile
                                      , bufferToFile'
                                      , runCConduit
                                      ) where

import Conduit
import Control.Applicative
import qualified Control.Concurrent.Async as A
import Control.Concurrent.Async.Lifted hiding (link2)
import Control.Concurrent.STM
import Control.Exception (finally)
import Control.Monad hiding (forM_)
import Control.Monad.Loops
import Control.Monad.Trans.Resource
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.Cereal as C
import qualified Data.Conduit.List as CL
import Data.Foldable (forM_)
import Data.Serialize
import Data.Void
import GHC.Prim
import System.Directory (removeFile)
import System.IO

-- | Concurrently join the producer and consumer, using a bounded queue of the
-- given size. The producer will block when the queue is full, if it is
-- producing faster than the consumers is taking from it. Likewise, if the
-- consumer races ahead, it will block until more input is available.
--
-- Exceptions are properly managed and propagated between the two sides, so
-- the net effect should be equivalent to not using buffer at all, save for
-- the concurrent interleaving of effects.
--
-- The underlying monad must always be an instance of
-- 'MonadBaseControl IO'.  If at least one of the two conduits is a
-- 'CFConduit', it must additionally be a in instance of
-- 'MonadResource'.
--
-- This function is similar to '$$'; for one more like '=$=', see
-- 'buffer''.
--
-- >>> buffer 1 (CL.sourceList [1,2,3]) CL.consume
-- [1,2,3]
buffer :: (CCatable c1 c2 c3, CRunnable c3, RunConstraints c3 m)
          => Int -- ^ Size of the bounded queue in memory.
          -> c1 () x m ()
          -> c2 x Void m r
          -> m r
buffer i c1 c2 = runCConduit (buffer' i c1 c2)

-- | An operator form of 'buffer'.  In general you should be able to replace
-- any use of '$$' with '$$&' and suddenly reap the benefit of
-- concurrency, if your conduits were spending time waiting on each other.
--
-- The underlying monad must always be an instance of
-- 'MonadBaseControl IO'.  If at least one of the two conduits is a
-- 'CFConduit', it must additionally be a in instance of
-- 'MonadResource'.
--
-- >>> CL.sourceList [1,2,3] $$& CL.consume
-- [1,2,3]
--
-- It can be combined with '$=&' and '$='.  This creates two threads;
-- the first thread produces the list and the second thread does the
-- map and the consume:
--
-- >>> CL.sourceList [1,2,3] $$& mapC (*2) $= CL.consume
-- [2,4,6]
--
-- This creates three threads.  The three conduits all run in their
-- own threads:
--
-- >>> CL.sourceList [1,2,3] $$& mapC (*2) $=& CL.consume
-- [2,4,6]
--
-- >>> CL.sourceList [1,2,3] $$& (mapC (*2) $= mapC (+1)) $=& CL.consume
-- [3,5,7]
($$&) :: (CCatable c1 c2 c3, CRunnable c3, RunConstraints c3 m) => c1 () x m () -> c2 x Void m r -> m r
a $$& b = runCConduit (a =$=& b)
infixr 0 $$&

-- | An operator form of 'buffer''.  In general you should be able to replace
-- any use of '=$=' with '=$=&' and '$$' either with '$$&' or '=$='
-- and 'runCConduit' and suddenly reap the benefit of concurrency, if
-- your conduits were spending time waiting on each other.
--
-- >>> runCConduit $ CL.sourceList [1,2,3] =$=& CL.consume
-- [1,2,3]
(=$=&) :: (CCatable c1 c2 c3) => c1 i x m () -> c2 x o m r -> c3 i o m r
a =$=& b = buffer' 64 a b
infixr 2 =$=&

-- | An alias for '=$=&' by analogy with '=$=' and '$='.
($=&) :: (CCatable c1 c2 c3) => c1 i x m () -> c2 x o m r -> c3 i o m r
($=&) = (=$=&)
infixl 1 $=&

-- | An alias for '=$=&' by analogy with '=$=' and '=$'.
(=$&) :: (CCatable c1 c2 c3) => c1 i x m () -> c2 x o m r -> c3 i o m r
(=$&) = (=$=&)
infixr 2 =$&

-- | Conduits are concatenable; this class describes how.
-- class CCatable (c1 :: * -> * -> (* -> *) -> * -> *) (c2 :: * -> * -> (* -> *) -> * -> *) (c3 :: * -> * -> (* -> *) -> * -> *) | c1 c2 -> c3 where
class CCatable c1 c2 (c3 :: * -> * -> (* -> *) -> * -> *) | c1 c2 -> c3 where
  -- | Concurrently join the producer and consumer, using a bounded queue of the
  -- given size. The producer will block when the queue is full, if it is
  -- producing faster than the consumers is taking from it. Likewise, if the
  -- consumer races ahead, it will block until more input is available.
  --
  -- Exceptions are properly managed and propagated between the two sides, so
  -- the net effect should be equivalent to not using buffer at all, save for
  -- the concurrent interleaving of effects.
  --
  -- This function is similar to '=$='; for one more like '$$', see
  -- 'buffer'.
  --
  -- >>> runCConduit $ buffer' 1 (CL.sourceList [1,2,3]) CL.consume
  -- [1,2,3]
  buffer' :: Int -- ^ Size of the bounded queue in memory
             -> c1 i x m ()
             -> c2 x o m r
             -> c3 i o m r

-- | Like 'buffer', except that when the bounded queue is overflowed, the
-- excess is cached in a local file so that consumption from upstream may
-- continue. When the queue becomes exhausted by yielding, it is filled
-- from the cache until all elements have been yielded.
--
-- Note that the maximum amount of memory consumed is equal to (2 *
-- memorySize + 1), so take this into account when picking a chunking size.
--
-- This function is similar to '$$'; for one more like '=$=', see
-- 'bufferToFile''.
--
-- >>> runResourceT $ bufferToFile 1 Nothing "/tmp" (CL.sourceList [1,2,3]) CL.consume
-- [1,2,3]
bufferToFile :: (CFConduitLike c1, CFConduitLike c2, Serialize x, MonadBaseControl IO m, MonadIO m, MonadResource m)
                => Int -- ^ Size of the bounded queue in memory
                -> Maybe Int -- ^ Max elements to keep on disk at one time
                -> FilePath -- ^ Directory to write temp files to
                -> c1 () x m ()
                -> c2 x Void m r
                -> m r
bufferToFile bufsz dsksz tmpDir c1 c2 = runCConduit (bufferToFile' bufsz dsksz tmpDir c1 c2)

-- | Like 'buffer'', except that when the bounded queue is overflowed, the
-- excess is cached in a local file so that consumption from upstream may
-- continue. When the queue becomes exhausted by yielding, it is filled
-- from the cache until all elements have been yielded.
--
-- Note that the maximum amount of memory consumed is equal to (2 *
-- memorySize + 1), so take this into account when picking a chunking size.
--
-- This function is similar to '=$='; for one more like '$$', see
-- 'bufferToFile'.
--
-- >>> runResourceT $ runCConduit $ bufferToFile' 1 Nothing "/tmp" (CL.sourceList [1,2,3]) CL.consume
-- [1,2,3]
--
-- It is frequently convenient to define local function to use this in operator form:
--
-- >>> :{
-- runResourceT $ do
--   let buf c = bufferToFile' 10 Nothing "/tmp" c -- eta-conversion to avoid monomorphism restriction
--   runCConduit $ CL.sourceList [0x30, 0x31, 0x32] `buf` mapC (toEnum :: Int -> Char) `buf` CL.consume
-- :}
-- "012"
bufferToFile' :: (CFConduitLike c1, CFConduitLike c2, Serialize x)
                 => Int -- ^ Size of the bounded queue in memory
                 -> Maybe Int -- ^ Max elements to keep on disk at one time
                 -> FilePath -- ^ Directory to write temp files to
                 -> c1 i x m ()
                 -> c2 x o m r
                 -> CFConduit i o m r
bufferToFile' bufsz dsksz tmpDir c1 c2 = combine (asCFConduit c1) (asCFConduit c2)
  where combine (FSingle a) b = FMultipleF bufsz dsksz tmpDir a b
        combine (FMultiple i a as) b = FMultiple i a (bufferToFile' bufsz dsksz tmpDir as b)
        combine (FMultipleF bufsz' dsksz' tmpDir' a as) b = FMultipleF bufsz' dsksz' tmpDir' a (bufferToFile' bufsz dsksz tmpDir as b)

-- | Conduits are, once there's a producer on one end and a consumer
-- on the other, runnable.
class CRunnable c where
  type RunConstraints c (m :: * -> *) :: Constraint
  -- | Execute a conduit concurrently.  This is the concurrent
  -- equivalent of 'runConduit'.
  --
  -- The underlying monad must always be an instance of
  -- 'MonadBaseControl IO'.  If the conduits is a 'CFConduit', it must
  -- additionally be a in instance of 'MonadResource'.
  runCConduit :: (RunConstraints c m) => c () Void m r -> m r

instance CCatable ConduitM ConduitM CConduit where
  buffer' i a b = buffer' i (Single a) (Single b)

instance CCatable ConduitM CConduit CConduit where
  buffer' i a b = buffer' i (Single a) b

instance CCatable ConduitM CFConduit CFConduit where
  buffer' i a b = buffer' i (asCFConduit a) b

instance CCatable CConduit ConduitM CConduit where
  buffer' i a b = buffer' i a (Single b)

instance CCatable CConduit CConduit CConduit where
  buffer' i (Single a) b = Multiple i a b
  buffer' i (Multiple i' a as) b = Multiple i' a (buffer' i as b)

instance CCatable CConduit CFConduit CFConduit where
  buffer' i a b = buffer' i (asCFConduit a) b

instance CCatable CFConduit ConduitM CFConduit where
  buffer' i a b = buffer' i a (asCFConduit b)

instance CCatable CFConduit CConduit CFConduit where
  buffer' i a b = buffer' i a (asCFConduit b)

instance CCatable CFConduit CFConduit CFConduit where
  buffer' i (FSingle a) b = FMultiple i a b
  buffer' i (FMultiple i' a as) b = FMultiple i' a (buffer' i as b)
  buffer' i (FMultipleF bufsz dsksz tmpDir a as) b = FMultipleF bufsz dsksz tmpDir a (buffer' i as b)

instance CRunnable ConduitM where
  type RunConstraints ConduitM m = (Monad m)
  runCConduit = runConduit

instance CRunnable CConduit where
  type RunConstraints CConduit m = (MonadBaseControl IO m, MonadIO m)
  runCConduit (Single c) = runConduit c
  runCConduit (Multiple bufsz c cs) = do
    chan <- liftIO $ newTBQueueIO bufsz
    withAsync (sender chan c) $ \c' ->
      stage chan c' cs

instance CRunnable CFConduit where
  type RunConstraints CFConduit m = (MonadBaseControl IO m, MonadIO m, MonadResource m)
  runCConduit (FSingle c) = runConduit c
  runCConduit (FMultiple bufsz c cs) = do
    chan <- liftIO $ newTBQueueIO bufsz
    withAsync (sender chan c) $ \c' ->
      fstage (receiver chan) c' cs
  runCConduit (FMultipleF bufsz filemax tempDir c cs) = do
    context <- liftIO $ BufferContext <$> newTBQueueIO bufsz
                                      <*> newTQueueIO
                                      <*> newTVarIO filemax
                                      <*> newTVarIO False
                                      <*> pure tempDir
    withAsync (fsender context c) $ \c' ->
      fstage (freceiver context) c' cs

-- | A "concurrent conduit", in which the stages run in parallel with
-- a buffering queue between them.
data CConduit i o m r where
  Single :: ConduitM i o m r -> CConduit i o m r
  Multiple :: Int -> ConduitM i x m () -> CConduit x o m r -> CConduit i o m r

-- C.C.A.L's link2 has the wrong type:  https://github.com/maoe/lifted-async/issues/16
link2 :: MonadBase IO m => Async a -> Async b -> m ()
link2 = (liftBase .) . A.link2

-- Combines a producer with a queue, sending it everything the
-- producer produces.
sender :: (MonadIO m) => TBQueue (Maybe o) -> ConduitM () o m () -> m ()
sender chan input = do
  input $$ mapM_C (send chan . Just)
  send chan Nothing

-- One "layer" of withAsync in a CConduit run.
stage :: (MonadBaseControl IO m, MonadIO m) => TBQueue (Maybe i) -> Async x -> CConduit i Void m r -> m r
stage chan prevAsync (Single c) =
  -- The last layer; feed the output of "chan" into the conduit and
  -- wait for the result.
  withAsync (receiver chan $$ c) $ \c' -> do
    link2 prevAsync c'
    wait c'
stage chan prevAsync (Multiple bufsz c cs) = do
  -- not the last layer, so take the input from "chan", have this
  -- layer's conduit process it, and send the conduit's output to the
  -- next layer.
  chan' <- liftIO $ newTBQueueIO bufsz
  withAsync (sender chan' $ receiver chan =$= c) $ \c' -> do
    link2 prevAsync c'
    stage chan' c' cs

-- A Producer which produces the values of the given channel until
-- Nothing is received.  This is the other half of "sender".
receiver :: (MonadIO m) => TBQueue (Maybe o) -> ConduitM () o m ()
receiver chan = do
  mx <- recv chan
  case mx of
   Nothing -> return ()
   Just x -> yield x >> receiver chan

-- | A "concurrent conduit", in which the stages run in parallel with
-- a buffering queue and possibly a disk file between them.
data CFConduit i o m r where
  FSingle :: ConduitM i o m r -> CFConduit i o m r
  FMultiple :: Int -> ConduitM i x m () -> CFConduit x o m r -> CFConduit i o m r
  FMultipleF :: (Serialize x) => Int -> Maybe Int -> FilePath -> ConduitM i x m () -> CFConduit x o m r -> CFConduit i o m r

class CFConduitLike a where
  asCFConduit :: a i o m r -> CFConduit i o m r

instance CFConduitLike ConduitM where
  asCFConduit = FSingle

instance CFConduitLike CConduit where
  asCFConduit (Single c) = FSingle c
  asCFConduit (Multiple i c cs) = FMultiple i c (asCFConduit cs)

instance CFConduitLike CFConduit where
  asCFConduit = id

data BufferContext m a = BufferContext { chan :: TBQueue a
                                       , restore :: TQueue (Source m a)
                                       , slotsFree :: TVar (Maybe Int)
                                       , done :: TVar Bool
                                       , tempDir :: FilePath
                                       }

-- The file-backed equivlent of "sender".  This sends the values
-- generated by "input" to the "chan" in the BufferContext until it
-- gets full, then flushes it to disk via "persistChan".
fsender :: (MonadIO m, MonadResource m, Serialize x) => BufferContext m x -> ConduitM () x m () -> m ()
fsender bc@BufferContext{..} input = do
  input $$ mapM_C $ \x -> join $ liftIO $ atomically $ do
    (writeTBQueue chan x >> return (return ())) `orElse` do
      action <- persistChan bc
      writeTBQueue chan x
      return action
  liftIO $ atomically $ writeTVar done True

-- Connect a stage to another stage via either an in-memory queue or a
-- disk buffer.  This is the file-backed equivalent of "stage".
fstage :: (MonadBaseControl IO m, MonadIO m, MonadResource m) => ConduitM () i m () -> Async x -> CFConduit i Void m r -> m r
fstage prevStage prevAsync (FSingle c) =
  -- The final conduit in the chain; just accept everything from
  -- the previous stage and wait for the result.
  withAsync (prevStage $$ c) $ \c' -> do
    link2 prevAsync c'
    wait c'
fstage prevStage prevAsync (FMultiple bufsz c cs) = do
  -- This stage is connected to the next via a non-file-backed
  -- channel, so it just uses "sender" and "reciever" in the same way
  -- "stage" does.
  chan' <- liftIO $ newTBQueueIO bufsz
  withAsync (sender chan' $ prevStage =$= c) $ \c' -> do
    link2 prevAsync c'
    fstage (receiver chan') c' cs
fstage prevStage prevAsync (FMultipleF bufsz dsksz tempDir c cs) = do
  -- This potentially needs to write its output to a file, so it uses
  -- "fsender" send and tells the next stage to use "freceiver" to read.
  bc <- liftIO $ BufferContext <$> newTBQueueIO bufsz
                               <*> newTQueueIO
                               <*> newTVarIO dsksz
                               <*> newTVarIO False
                               <*> pure tempDir
  withAsync (fsender bc $ prevStage =$= c) $ \c' -> do
    link2 prevAsync c'
    fstage (freceiver bc) c' cs

-- Receives from disk files or the in-memory queue if no spill-to-disk
-- has occurred.
freceiver :: (MonadIO m) => BufferContext m o -> ConduitM () o m ()
freceiver BufferContext{..} = loop where
  loop = do
    (src, exit) <- liftIO $ atomically $ do
      (readTQueue restore >>= (\action -> return (action, False))) `orElse` do
        xs <- exhaust chan
        isDone <- readTVar done
        return (CL.sourceList xs, isDone)
    src
    unless exit loop

-- The channel is full, so (return an action which will) spill it to disk, unless too
-- many items are there already.
persistChan :: (MonadIO m, MonadResource m, Serialize o) => BufferContext m o -> STM (m ())
persistChan BufferContext{..} = do
  xs <- exhaust chan
  mslots <- readTVar slotsFree
  let len = length xs
  forM_ mslots $ \slots -> check (len < slots)
  filePath <- newEmptyTMVar
  writeTQueue restore $ do
    (path, key) <- liftIO $ atomically $ takeTMVar filePath
    CB.sourceFile path $= do
      C.conduitGet get
      liftIO $ atomically $ modifyTVar slotsFree (fmap (+ len))
      release key
  case xs of
   [] -> return (return ())
   _ -> do
     modifyTVar slotsFree (fmap (subtract len))
     return $ do
       (key, (path, h)) <- allocate (openBinaryTempFile tempDir "conduit.bin") (\(path, h) -> hClose h `finally` removeFile path)
       liftIO $ do
         CL.sourceList xs $= C.conduitPut put $$ CB.sinkHandle h
         hClose h
         atomically $ putTMVar filePath (path, key)

exhaust :: TBQueue a -> STM [a]
exhaust chan = whileM (not <$> isEmptyTBQueue chan) (readTBQueue chan)

recv :: (MonadIO m) => TBQueue a -> m a
recv c = liftIO . atomically $ readTBQueue c

send :: (MonadIO m) => TBQueue a -> a -> m ()
send c = liftIO . atomically . writeTBQueue c
