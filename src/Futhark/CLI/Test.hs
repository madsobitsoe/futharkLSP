{-# LANGUAGE OverloadedStrings, FlexibleContexts, LambdaCase #-}
-- | This program is a convenience utility for running the Futhark
-- test suite, and its test programs.
module Futhark.CLI.Test (main) where

import Control.Applicative.Lift (runErrors, failure, Errors, Lift(..))
import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.Except hiding (throwError)
import qualified Control.Monad.Except as E
import qualified Data.ByteString as SBS
import qualified Data.ByteString.Lazy as LBS

import Data.List
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T
import System.Console.ANSI
import System.Process.ByteString (readProcessWithExitCode)
import System.Environment
import System.Exit
import System.FilePath
import System.Console.GetOpt
import qualified System.Console.Terminal.Size as Terminal
import System.IO
import Text.Regex.TDFA

import Futhark.Analysis.Metrics
import Futhark.Test
import Futhark.Util.Options
import Futhark.Util.Pretty (prettyText)
import Futhark.Util.Table

--- Test execution

type TestM = ExceptT [T.Text] IO

-- Taken from transformers-0.5.5.0.
eitherToErrors :: Either e a -> Errors e a
eitherToErrors = either failure Pure

throwError :: MonadError [e] m => e -> m a
throwError e = E.throwError [e]

runTestM :: TestM () -> IO TestResult
runTestM = fmap (either Failure $ const Success) . runExceptT

io :: IO a -> TestM a
io = liftIO

context :: T.Text -> TestM a -> TestM a
context s = withExceptT $
  \case
    []      -> []
    (e:es') -> (s <> ":\n" <> e):es'

accErrors :: [TestM a] -> TestM [a]
accErrors tests = do
  eithers <- lift $ mapM runExceptT tests
  let errors = traverse eitherToErrors eithers
  ExceptT $ return $ runErrors errors

accErrors_ :: [TestM a] -> TestM ()
accErrors_ = void . accErrors

data TestResult = Success
                | Failure [T.Text]
                deriving (Eq, Show)

data TestCase = TestCase { _testCaseMode :: TestMode
                         , testCaseProgram :: FilePath
                         , testCaseTest :: ProgramTest
                         , _testCasePrograms :: ProgConfig
                         }
                deriving (Show)

instance Eq TestCase where
  x == y = testCaseProgram x == testCaseProgram y

instance Ord TestCase where
  x `compare` y = testCaseProgram x `compare` testCaseProgram y

data RunResult = ErrorResult Int SBS.ByteString
               | SuccessResult [Value]

progNotFound :: T.Text -> T.Text
progNotFound s = s <> ": command not found"

optimisedProgramMetrics :: ProgConfig -> StructurePipeline -> FilePath -> TestM AstMetrics
optimisedProgramMetrics programs pipeline program =
  case pipeline of SOACSPipeline ->
                     check "-s"
                   KernelsPipeline ->
                     check "--kernels"
                   SequentialCpuPipeline ->
                     check "--cpu"
                   GpuPipeline ->
                     check "--gpu"
  where check opt = do
          futhark <- io $ maybe getExecutablePath return $ configFuthark programs
          (code, output, err) <-
            io $ readProcessWithExitCode futhark ["dev", opt, "--metrics", program] ""
          let output' = T.decodeUtf8 output
          case code of
            ExitSuccess
              | [(m, [])] <- reads $ T.unpack output' -> return m
              | otherwise -> throwError $ "Could not read metrics output:\n" <> output'
            ExitFailure 127 -> throwError $ progNotFound $ T.pack futhark
            ExitFailure _ -> throwError $ T.decodeUtf8 err

testMetrics :: ProgConfig -> FilePath -> StructureTest -> TestM ()
testMetrics programs program (StructureTest pipeline (AstMetrics expected)) =
  context "Checking metrics" $ do
    actual <- optimisedProgramMetrics programs pipeline program
    accErrors_ $ map (ok actual) $ M.toList expected
  where ok (AstMetrics metrics) (name, expected_occurences) =
          case M.lookup name metrics of
            Nothing
              | expected_occurences > 0 ->
              throwError $ name <> " should have occurred " <> T.pack (show expected_occurences) <>
              " times, but did not occur at all in optimised program."
            Just actual_occurences
              | expected_occurences /= actual_occurences ->
                throwError $ name <> " should have occurred " <> T.pack (show expected_occurences) <>
              " times, but occurred " <> T.pack (show actual_occurences) <> " times."
            _ -> return ()

testWarnings :: [WarningTest] -> SBS.ByteString -> TestM ()
testWarnings warnings futerr = accErrors_ $ map testWarning warnings
  where testWarning (ExpectedWarning regex_s regex)
          | not (match regex $ T.unpack $ T.decodeUtf8 futerr) =
            throwError $ "Expected warning:\n  " <> regex_s <>
            "\nGot warnings:\n  " <> T.decodeUtf8 futerr
          | otherwise = return ()

runTestCase :: TestCase -> TestM ()
runTestCase (TestCase mode program testcase progs) = do
  futhark <- io $ maybe getExecutablePath return $ configFuthark progs
  case testAction testcase of

    CompileTimeFailure expected_error ->
      context (mconcat ["Type-checking with '", T.pack futhark,
                        " check ", T.pack program, "'"]) $ do
        (code, _, err) <-
          io $ readProcessWithExitCode futhark ["check", program] ""
        case code of
         ExitSuccess -> throwError "Expected failure\n"
         ExitFailure 127 -> throwError $ progNotFound $ T.pack futhark
         ExitFailure 1 -> throwError $ T.decodeUtf8 err
         ExitFailure _ -> checkError expected_error err

    RunCases _ _ warnings | mode == TypeCheck -> do
      let options = ["check", program] ++ configExtraCompilerOptions progs
      context (mconcat ["Type-checking with '", T.pack futhark,
                        " check ", T.pack program, "'"]) $ do
        (code, _, err) <- io $ readProcessWithExitCode futhark options ""
        testWarnings warnings err
        case code of
         ExitSuccess -> return ()
         ExitFailure 127 -> throwError $ progNotFound $ T.pack futhark
         ExitFailure _ -> throwError $ T.decodeUtf8 err

    RunCases ios structures warnings -> do
      -- Compile up-front and reuse same executable for several entry points.
      let backend = configBackend progs
          extra_options = configExtraCompilerOptions progs
      unless (mode == Compile) $
        context "Generating reference outputs" $
        ensureReferenceOutput futhark "c" program ios
      unless (mode == Interpreted) $
        context ("Compiling with --backend=" <> T.pack backend) $ do
          compileTestProgram extra_options futhark backend program warnings
          mapM_ (testMetrics progs program) structures
          unless (mode == Compile) $ do
            (tuning_opts, _) <-
              liftIO $ determineTuning (configTuning progs) program
            let progs' = progs { configExtraOptions =
                                 tuning_opts ++ configExtraOptions progs }
            context "Running compiled program" $
              accErrors_ $ map (runCompiledEntry program progs') ios
      unless (mode == Compile || mode == Compiled) $
        context "Interpreting" $
          accErrors_ $ map (runInterpretedEntry futhark program) ios

runInterpretedEntry :: String -> FilePath -> InputOutputs -> TestM()
runInterpretedEntry futhark program (InputOutputs entry run_cases) =
  let dir = takeDirectory program
      runInterpretedCase run@(TestRun _ inputValues _ index _) =
        unless ("compiled" `elem` runTags run) $
          context ("Entry point: " <> entry
                   <> "; dataset: " <> T.pack (runDescription run)) $ do

            input <- T.unlines . map prettyText <$> getValues dir inputValues
            expectedResult' <- getExpectedResult program entry run
            (code, output, err) <-
              io $ readProcessWithExitCode futhark ["run", "-e", T.unpack entry, program] $
              T.encodeUtf8 input
            case code of
              ExitFailure 127 -> throwError $ progNotFound $ T.pack futhark

              _               -> compareResult entry index program expectedResult'
                                 =<< runResult program code output err

  in accErrors_ $ map runInterpretedCase run_cases

runCompiledEntry :: FilePath -> ProgConfig -> InputOutputs -> TestM ()
runCompiledEntry program progs (InputOutputs entry run_cases) =
  -- Explicitly prefixing the current directory is necessary for
  -- readProcessWithExitCode to find the binary when binOutputf has
  -- no path component.
  let binOutputf = dropExtension program
      binpath = "." </> binOutputf
      entry_options = ["-e", T.unpack entry]

      runner = configRunner progs
      extra_options = configExtraOptions progs

      runCompiledCase run@(TestRun _ inputValues _ index _) =
        context ("Entry point: " <> entry
                 <> "; dataset: " <> T.pack (runDescription run)) $ do
          expected <- getExpectedResult program entry run
          (progCode, output, progerr) <-
            runProgram runner extra_options program entry inputValues
          compareResult entry index program expected
            =<< runResult program progCode output progerr

  in context ("Running " <> T.pack (unwords $ binpath : entry_options ++ extra_options)) $
     accErrors_ $ map runCompiledCase run_cases

checkError :: ExpectedError -> SBS.ByteString -> TestM ()
checkError (ThisError regex_s regex) err
  | not (match regex $ T.unpack $ T.decodeUtf8 err) =
     throwError $ "Expected error:\n  " <> regex_s <>
     "\nGot error:\n  " <> T.decodeUtf8 err
checkError _ _ =
  return ()

runResult :: FilePath -> ExitCode -> SBS.ByteString -> SBS.ByteString -> TestM RunResult
runResult program ExitSuccess stdout_s _ =
  case valuesFromByteString "stdout" $ LBS.fromStrict stdout_s of
    Left e   -> do
      let actualf = program `addExtension` "actual"
      io $ SBS.writeFile actualf stdout_s
      throwError $ T.pack e <> "\n(See " <> T.pack actualf <> ")"
    Right vs -> return $ SuccessResult vs
runResult _ (ExitFailure code) _ stderr_s =
  return $ ErrorResult code stderr_s

compileTestProgram :: [String] -> FilePath -> String -> FilePath -> [WarningTest] -> TestM ()
compileTestProgram extra_options futhark backend program warnings = do
  (_, futerr) <- compileProgram extra_options futhark backend program
  testWarnings warnings futerr

compareResult :: T.Text -> Int -> FilePath -> ExpectedResult [Value] -> RunResult
              -> TestM ()
compareResult _ _ _ (Succeeds Nothing) SuccessResult{} =
  return ()
compareResult entry index program (Succeeds (Just expectedResult)) (SuccessResult actualResult) =
  case compareValues1 actualResult expectedResult of
    Just mismatch -> do
      let actualf = program <.> T.unpack entry <.> show index <.> "actual"
          expectedf = program <.> T.unpack entry <.> show index <.> "expected"
      io $ SBS.writeFile actualf $
        T.encodeUtf8 $ T.unlines $ map prettyText actualResult
      io $ SBS.writeFile expectedf $
        T.encodeUtf8 $ T.unlines $ map prettyText expectedResult
      throwError $ T.pack actualf <> " and " <> T.pack expectedf <>
        " do not match:\n" <> T.pack (show mismatch) <> "\n"
    Nothing ->
      return ()
compareResult _ _ _ (RunTimeFailure expectedError) (ErrorResult _ actualError) =
  checkError expectedError actualError
compareResult _ _ _ (Succeeds _) (ErrorResult code err) =
  throwError $ "Program failed with error code " <>
  T.pack (show code) <> " and stderr:\n  " <> T.decodeUtf8 err
compareResult _ _ _ (RunTimeFailure f) (SuccessResult _) =
  throwError $ "Program succeeded, but expected failure:\n  " <> T.pack (show f)

---
--- Test manager
---

data TestStatus = TestStatus { testStatusRemain :: [TestCase]
                             , testStatusRun :: [TestCase]
                             , testStatusTotal :: Int
                             , testStatusFail :: Int
                             , testStatusPass :: Int
                             , testStatusRuns :: Int
                             , testStatusRunsRemain :: Int
                             , testStatusRunPass :: Int
                             , testStatusRunFail :: Int
                             }

catching :: IO TestResult -> IO TestResult
catching m = m `catch` save
  where save :: SomeException -> IO TestResult
        save e = return $ Failure [T.pack $ show e]

doTest :: TestCase -> IO TestResult
doTest = catching . runTestM . runTestCase

makeTestCase :: TestConfig -> TestMode -> (FilePath, ProgramTest) -> TestCase
makeTestCase config mode (file, spec) =
  TestCase mode file spec $ configPrograms config

data ReportMsg = TestStarted TestCase
               | TestDone TestCase TestResult

runTest :: MVar TestCase -> MVar ReportMsg -> IO ()
runTest testmvar resmvar = forever $ do
  test <- takeMVar testmvar
  putMVar resmvar $ TestStarted test
  res <- doTest test
  putMVar resmvar $ TestDone test res

excludedTest :: TestConfig -> TestCase -> Bool
excludedTest config =
  any (`elem` configExclude config) . testTags . testCaseTest

-- | Exclude those test cases that have tags we do not wish to run.
excludeCases :: TestConfig -> TestCase -> TestCase
excludeCases config tcase =
  tcase { testCaseTest = onTest $ testCaseTest tcase }
  where onTest (ProgramTest desc tags action) =
          ProgramTest desc tags $ onAction action
        onAction (RunCases ios stest wtest) =
          RunCases (map onIOs ios) stest wtest
        onAction action = action
        onIOs (InputOutputs entry runs) =
          InputOutputs entry $ filter (not . any excluded . runTags) runs
        excluded = (`elem` configExclude config) . T.pack

statusTable :: TestStatus -> String
statusTable ts = buildTable rows 1
  where rows =
          [ [ mkEntry "", passed, failed, mkEntry "remaining"]
          , map mkEntry ["programs", passedProgs, failedProgs, remainProgs']
          , map mkEntry ["runs", passedRuns, failedRuns, remainRuns']
          ]
        passed       = ("passed", [SetColor Foreground Vivid Green])
        failed       = ("failed", [SetColor Foreground Vivid Red])
        passedProgs  = show $ testStatusPass ts
        failedProgs  = show $ testStatusFail ts
        totalProgs   = show $ testStatusTotal ts
        totalRuns    = show $ testStatusRuns ts
        passedRuns   = show $ testStatusRunPass ts
        failedRuns   = show $ testStatusRunFail ts
        remainProgs  = show . length $ testStatusRemain ts
        remainProgs' = remainProgs ++ "/" ++ totalProgs
        remainRuns   = show $ testStatusRunsRemain ts
        remainRuns'  = remainRuns ++ "/" ++ totalRuns

tableLines :: Int
tableLines = 1 + (length . lines $ blankTable)
  where blankTable = statusTable $ TestStatus [] [] 0 0 0 0 0 0 0

spaceTable :: IO ()
spaceTable = putStr $ replicate tableLines '\n'

reportTable :: TestStatus -> IO ()
reportTable ts = do
  moveCursorToTableTop
  putStrLn $ statusTable ts
  clearLine
  w <- maybe 80 Terminal.width <$> Terminal.size
  putStrLn $ atMostChars (w-length labelstr) running
  where running = labelstr ++ (unwords . reverse . map testCaseProgram . testStatusRun) ts
        labelstr = "Now testing: "

moveCursorToTableTop :: IO ()
moveCursorToTableTop = cursorUpLine tableLines

atMostChars :: Int -> String -> String
atMostChars n s | length s > n = take (n-3) s ++ "..."
                | otherwise    = s

reportText :: TestStatus -> IO ()
reportText ts =
  putStr $ "(" ++ show (testStatusFail ts)  ++ " failed, " ++
                  show (testStatusPass ts)  ++ " passed, " ++
                  show num_remain           ++ " to go).\n"
    where num_remain  = length $ testStatusRemain ts

runTests :: TestConfig -> [FilePath] -> IO ()
runTests config paths = do
  -- We force line buffering to ensure that we produce running output.
  -- Otherwise, CI tools and the like may believe we are hung and kill
  -- us.
  hSetBuffering stdout LineBuffering

  let mode = configTestMode config
  all_tests <- map (makeTestCase config mode) <$>
               testSpecsFromPathsOrDie paths
  testmvar <- newEmptyMVar
  reportmvar <- newEmptyMVar
  concurrency <- getNumCapabilities
  replicateM_ concurrency $ forkIO $ runTest testmvar reportmvar

  let (excluded, included) = partition (excludedTest config) all_tests
  _ <- forkIO $ mapM_ (putMVar testmvar . excludeCases config) included
  isTTY <- (&& not (configLineOutput config)) <$> hIsTerminalDevice stdout

  let report | isTTY = reportTable
             | otherwise = reportText
      clear | isTTY = clearFromCursorToScreenEnd
            |otherwise = putStr "\n"

      numTestCases tc =
        case testAction $ testCaseTest tc of
          CompileTimeFailure _ -> 1
          RunCases ios sts wts -> (length . concat) (iosTestRuns <$> ios)
                                  + length sts + length wts

      getResults ts
        | null (testStatusRemain ts) = report ts >> return ts
        | otherwise = do
          report ts
          msg <- takeMVar reportmvar
          case msg of
            TestStarted test -> do
              unless isTTY $
                putStr $ "Started testing " <> testCaseProgram test <> " "
              getResults $ ts {testStatusRun = test : testStatusRun ts}
            TestDone test res -> do
              let ts' = ts { testStatusRemain = test `delete` testStatusRemain ts
                           , testStatusRun    = test `delete` testStatusRun ts
                           , testStatusRunsRemain = testStatusRunsRemain ts
                                                    - numTestCases test
                           }
              case res of
                Success -> do
                  let ts'' = ts' { testStatusRunPass =
                                     testStatusRunPass ts' + numTestCases test
                                 }
                  unless isTTY $
                    putStr $ "Finished testing " <> testCaseProgram test <> " "
                  getResults $ ts'' { testStatusPass = testStatusPass ts + 1}
                Failure s -> do
                  when isTTY moveCursorToTableTop
                  clear
                  T.putStr $ (T.pack (inRed $ testCaseProgram test) <> ":\n") <> T.unlines s
                  when isTTY spaceTable
                  getResults $ ts' { testStatusFail = testStatusFail ts' + 1
                                   , testStatusRunPass = testStatusRunPass ts'
                                                         + numTestCases test - length s

                                   , testStatusRunFail = testStatusRunFail ts'
                                                         + length s
                                   }

  when isTTY spaceTable

  ts <- getResults TestStatus { testStatusRemain = included
                              , testStatusRun    = []
                              , testStatusTotal  = length included
                              , testStatusFail   = 0
                              , testStatusPass   = 0
                              , testStatusRuns  = sum $ map numTestCases included
                              , testStatusRunsRemain = sum $ map numTestCases included
                              , testStatusRunPass = 0
                              , testStatusRunFail = 0
                              }

  -- Removes "Now testing" output.
  when isTTY $ cursorUpLine 1 >> clearLine

  let excluded_str | null excluded = ""
                   | otherwise = " (" ++ show (length excluded) ++ " program(s) excluded).\n"
  putStr excluded_str
  exitWith $ case testStatusFail ts of 0 -> ExitSuccess
                                       _ -> ExitFailure 1

inRed :: String -> String
inRed s = setSGRCode [SetColor Foreground Vivid Red] ++ s ++ setSGRCode [Reset]

---
--- Configuration and command line parsing
---

data TestConfig = TestConfig
                  { configTestMode :: TestMode
                  , configPrograms :: ProgConfig
                  , configExclude :: [T.Text]
                  , configLineOutput :: Bool
                  }

defaultConfig :: TestConfig
defaultConfig = TestConfig { configTestMode = Everything
                           , configExclude = [ "disable" ]
                           , configPrograms =
                             ProgConfig
                             { configBackend = "c"
                             , configFuthark = Nothing
                             , configRunner = ""
                             , configExtraOptions = []
                             , configExtraCompilerOptions = []
                             , configTuning = Just "tuning"
                             }
                           , configLineOutput = False
                           }

data ProgConfig = ProgConfig
                  { configBackend :: String
                  , configFuthark :: Maybe FilePath
                  , configRunner :: FilePath
                  , configExtraCompilerOptions :: [String]
                  , configTuning :: Maybe String
                  , configExtraOptions :: [String]
                  -- ^ Extra options passed to the programs being run.
                  }
                  deriving (Show)

changeProgConfig :: (ProgConfig -> ProgConfig) -> TestConfig -> TestConfig
changeProgConfig f config = config { configPrograms = f $ configPrograms config }

setBackend :: FilePath -> ProgConfig -> ProgConfig
setBackend backend config =
  config { configBackend = backend }

setFuthark :: FilePath -> ProgConfig -> ProgConfig
setFuthark futhark config =
  config { configFuthark = Just futhark }

setRunner :: FilePath -> ProgConfig -> ProgConfig
setRunner runner config =
  config { configRunner = runner }

addCompilerOption :: String -> ProgConfig -> ProgConfig
addCompilerOption option config =
  config { configExtraCompilerOptions = configExtraCompilerOptions config ++ [option] }

addOption :: String -> ProgConfig -> ProgConfig
addOption option config =
  config { configExtraOptions = configExtraOptions config ++ [option] }

data TestMode = TypeCheck
              | Compile
              | Compiled
              | Interpreted
              | Everything
              deriving (Eq, Show)

commandLineOptions :: [FunOptDescr TestConfig]
commandLineOptions = [
    Option "t" ["typecheck"]
    (NoArg $ Right $ \config -> config { configTestMode = TypeCheck })
    "Only perform type-checking"
  , Option "i" ["interpreted"]
    (NoArg $ Right $ \config -> config { configTestMode = Interpreted })
    "Only interpret"
  , Option "c" ["compiled"]
    (NoArg $ Right $ \config -> config { configTestMode = Compiled })
    "Only run compiled code"
  , Option "C" ["compile"]
    (NoArg $ Right $ \config -> config { configTestMode = Compile })
    "Only compile, do not run."
  , Option [] ["no-terminal", "notty"]
    (NoArg $ Right $ \config -> config { configLineOutput = True })
    "Provide simpler line-based output."
  , Option [] ["backend"]
    (ReqArg (Right . changeProgConfig . setBackend) "BACKEND")
    "Backend used for compilation (defaults to 'c')."
  , Option [] ["futhark"]
    (ReqArg (Right . changeProgConfig . setFuthark) "PROGRAM")
    "Program to run for subcommands (defaults to same binary as 'futhark test')."
  , Option [] ["runner"]
    (ReqArg (Right . changeProgConfig . setRunner) "PROGRAM")
    "The program used to run the Futhark-generated programs (defaults to nothing)."
  , Option [] ["exclude"]
    (ReqArg (\tag ->
               Right $ \config ->
               config { configExclude = T.pack tag : configExclude config })
     "TAG")
    "Exclude test programs that define this tag."
  , Option "p" ["pass-option"]
    (ReqArg (Right . changeProgConfig . addOption) "OPT")
    "Pass this option to programs being run."
  , Option [] ["pass-compiler-option"]
    (ReqArg (Right . changeProgConfig . addCompilerOption) "OPT")
    "Pass this option to the compiler (or typechecker if in -t mode)."
  , Option [] ["no-tuning"]
    (NoArg $ Right $ changeProgConfig $ \config -> config { configTuning = Nothing })
    "Do not load tuning files."
  ]

main :: String -> [String] -> IO ()
main = mainWithOptions defaultConfig commandLineOptions "options... programs..." $ \progs config ->
  Just $ runTests config progs
