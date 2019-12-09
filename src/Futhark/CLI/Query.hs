module Futhark.CLI.Query (main) where

import Text.Read (readMaybe)

import Data.Loc
import Futhark.Compiler
import Futhark.Util.Options
import Language.Futhark.Query
import Language.Futhark.Syntax

main :: String -> [String] -> IO ()
main = mainWithOptions () [] "program line:col" $ \args () ->
  case args of
    [file, line, col] -> do
      line' <- readMaybe line
      col' <- readMaybe col
      Just $ do
        (_, imports, _) <- readProgramOrDie file
        -- The 'offset' part of the Pos is not used and can be arbitrary.
        case atPos imports $ Pos file line' col' 0 of
          Nothing -> putStrLn "No information available."
          Just (AtName qn def loc) -> do
            putStrLn $ pretty qn
            putStrLn $ locStr loc
            case def of
              Nothing -> return ()
              Just (BoundTerm t defloc) -> do
                putStrLn $ "Type: " ++ pretty t
                putStrLn $ "Defined at: " ++ locStr (srclocOf defloc)
    _ -> Nothing
