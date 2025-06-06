-- DEPRECATED: This Haskell parser is now deprecated in favor of SQLGlot.
-- This file is kept for backward compatibility but should not be used.

{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.Aeson (encode, object, (.=))
import qualified Data.ByteString.Lazy.Char8 as BL
import System.Environment (getArgs)
import System.Exit (exitFailure, exitSuccess)
import System.IO (hPutStrLn, stderr)

main :: IO ()
main = do
  args <- getArgs
  case args of
    [sql] -> do
      -- Return deprecation message
      BL.putStrLn $ encode $ object [
        "tag" .= ("Error" :: String), 
        "contents" .= ("Haskell parser is deprecated. Use SQLGlot parser instead." :: String)
      ]
      exitFailure
    _ -> do
      hPutStrLn stderr "DEPRECATED: This Haskell parser is no longer supported."
      hPutStrLn stderr "Use SQLGlot parser instead."
      exitFailure