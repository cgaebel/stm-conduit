module Main
  where

import Test.DocTest
import System.FilePath.Find ((==?), always, extension, find)

find_sources :: IO [FilePath]
find_sources = find always (extension ==? ".hs") "Data"

main :: IO ()
main = do
  sources <- find_sources
  doctest $ sources
