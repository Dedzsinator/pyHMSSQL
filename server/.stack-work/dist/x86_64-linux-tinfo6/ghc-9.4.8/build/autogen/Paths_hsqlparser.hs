{-# LANGUAGE CPP #-}
{-# LANGUAGE NoRebindableSyntax #-}
{-# OPTIONS_GHC -fno-warn-missing-import-lists #-}
{-# OPTIONS_GHC -w #-}
module Paths_hsqlparser (
    version,
    getBinDir, getLibDir, getDynLibDir, getDataDir, getLibexecDir,
    getDataFileName, getSysconfDir
  ) where


import qualified Control.Exception as Exception
import qualified Data.List as List
import Data.Version (Version(..))
import System.Environment (getEnv)
import Prelude


#if defined(VERSION_base)

#if MIN_VERSION_base(4,0,0)
catchIO :: IO a -> (Exception.IOException -> IO a) -> IO a
#else
catchIO :: IO a -> (Exception.Exception -> IO a) -> IO a
#endif

#else
catchIO :: IO a -> (Exception.IOException -> IO a) -> IO a
#endif
catchIO = Exception.catch

version :: Version
version = Version [0,1,0,0] []

getDataFileName :: FilePath -> IO FilePath
getDataFileName name = do
  dir <- getDataDir
  return (dir `joinFileName` name)

getBinDir, getLibDir, getDynLibDir, getDataDir, getLibexecDir, getSysconfDir :: IO FilePath



bindir, libdir, dynlibdir, datadir, libexecdir, sysconfdir :: FilePath
bindir     = "/home/deginandor/Documents/Programming/pyHMSSQL/server/.stack-work/install/x86_64-linux-tinfo6/61e1d6b6b8a52e8ead991db50d586d4970da24748b9717067065a6beb758f66e/9.4.8/bin"
libdir     = "/home/deginandor/Documents/Programming/pyHMSSQL/server/.stack-work/install/x86_64-linux-tinfo6/61e1d6b6b8a52e8ead991db50d586d4970da24748b9717067065a6beb758f66e/9.4.8/lib/x86_64-linux-ghc-9.4.8/hsqlparser-0.1.0.0-6kVXX5siyil3nyrXDk8K0W"
dynlibdir  = "/home/deginandor/Documents/Programming/pyHMSSQL/server/.stack-work/install/x86_64-linux-tinfo6/61e1d6b6b8a52e8ead991db50d586d4970da24748b9717067065a6beb758f66e/9.4.8/lib/x86_64-linux-ghc-9.4.8"
datadir    = "/home/deginandor/Documents/Programming/pyHMSSQL/server/.stack-work/install/x86_64-linux-tinfo6/61e1d6b6b8a52e8ead991db50d586d4970da24748b9717067065a6beb758f66e/9.4.8/share/x86_64-linux-ghc-9.4.8/hsqlparser-0.1.0.0"
libexecdir = "/home/deginandor/Documents/Programming/pyHMSSQL/server/.stack-work/install/x86_64-linux-tinfo6/61e1d6b6b8a52e8ead991db50d586d4970da24748b9717067065a6beb758f66e/9.4.8/libexec/x86_64-linux-ghc-9.4.8/hsqlparser-0.1.0.0"
sysconfdir = "/home/deginandor/Documents/Programming/pyHMSSQL/server/.stack-work/install/x86_64-linux-tinfo6/61e1d6b6b8a52e8ead991db50d586d4970da24748b9717067065a6beb758f66e/9.4.8/etc"

getBinDir     = catchIO (getEnv "hsqlparser_bindir")     (\_ -> return bindir)
getLibDir     = catchIO (getEnv "hsqlparser_libdir")     (\_ -> return libdir)
getDynLibDir  = catchIO (getEnv "hsqlparser_dynlibdir")  (\_ -> return dynlibdir)
getDataDir    = catchIO (getEnv "hsqlparser_datadir")    (\_ -> return datadir)
getLibexecDir = catchIO (getEnv "hsqlparser_libexecdir") (\_ -> return libexecdir)
getSysconfDir = catchIO (getEnv "hsqlparser_sysconfdir") (\_ -> return sysconfdir)




joinFileName :: String -> String -> FilePath
joinFileName ""  fname = fname
joinFileName "." fname = fname
joinFileName dir ""    = dir
joinFileName dir fname
  | isPathSeparator (List.last dir) = dir ++ fname
  | otherwise                       = dir ++ pathSeparator : fname

pathSeparator :: Char
pathSeparator = '/'

isPathSeparator :: Char -> Bool
isPathSeparator c = c == '/'
