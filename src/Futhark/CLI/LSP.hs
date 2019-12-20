{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | A Language Server Protocol server for Futhark.
module Futhark.CLI.LSP (main) where

import           Control.Concurrent
import           Control.Concurrent.STM.TChan
import qualified Control.Exception                     as E
import           Control.Lens
import           Control.Monad
import           Control.Monad.IO.Class
import           Control.Monad.Reader
import           Control.Monad.STM
import qualified Data.Aeson                            as J
import           Data.Default
import qualified Data.HashMap.Strict                   as H
import qualified Data.Text                             as T
import qualified Language.Haskell.LSP.Control          as CTRL
import qualified Language.Haskell.LSP.Core             as Core
import           Language.Haskell.LSP.Diagnostics
import           Language.Haskell.LSP.Messages
import qualified Language.Haskell.LSP.Types            as J
import qualified Language.Haskell.LSP.Types.Lens       as J
import qualified Language.Haskell.LSP.Utility          as U
import           Language.Haskell.LSP.VFS
import           System.Exit
import qualified System.Log.Logger                     as L
import qualified Data.Rope.UTF16                       as Rope
-- Mads adding imports
import Futhark.Compiler (readProgram, readProgramOrDie, Imports)
import Futhark.Pipeline (runFutharkM, Verbosity(..))
import Data.Loc
import Language.Futhark.Query
import Language.Futhark.Syntax
import Text.Read (readMaybe)
import Debug.Trace
import System.IO
--  Mads done importing stuff
import           Futhark.Util.Options (mainWithOptions)
import qualified System.Random                         as Rng
import Text.Regex
import Control.Monad.State (StateT, get, put, modify, evalStateT)

main :: String -> [String] -> IO ()
main = mainWithOptions () [] "" $ \args () -> do
  guard $ null args
  Just $ do
    status <- run (return ())
    case status of
      0 -> exitSuccess
      c -> exitWith . ExitFailure $ c

-- dump the warnings from the compiler

dump :: String -> IO ()
dump s = hPutStrLn stderr s

dumpStatus :: Maybe String -> IO ()
dumpStatus file = 
  case file of
    Nothing -> hPutStr stderr "No filename supplied\n"
    Just filename -> do
      res <- runFutharkM (readProgram filename) NotVerbose
      case res of
        Left _ -> hPutStr stderr "failure\n"
        Right (w,_,_) -> hPutStr stderr $ (++) "Success!\n" $ show w


run :: IO () -> IO Int
run dispatcherProc = flip E.catches handlers $ do

  rin  <- atomically newTChan :: IO (TChan ReactorInput)

  let
    dp lf = do
      liftIO $ U.logs "main.run:dp entered"
      _rpid  <- forkIO $ reactor lf rin
      liftIO $ U.logs "main.run:dp tchan"
      dispatcherProc
      liftIO $ U.logs "main.run:dp after dispatcherProc"
      return Nothing

    callbacks =
      Core.InitializeCallbacks { Core.onInitialConfiguration = \_ -> Right ()
                               , Core.onConfigurationChange = \_ -> Right ()
                               , Core.onStartup = dp
                               }

  flip E.finally finalProc $ do
    Core.setupLogger (Just "/tmp/lsp-hello.log") [] L.DEBUG
    CTRL.run callbacks (lspHandlers rin) lspOptions (Just "/tmp/lsp-hello-session.log")

  where
    handlers = [ E.Handler ioExcept
               , E.Handler someExcept
               ]
    finalProc = L.removeAllHandlers
    ioExcept   (e :: E.IOException)       = print e >> return 1
    someExcept (e :: E.SomeException)     = print e >> return 1

-- The reactor is a process that serialises and buffers all requests from the
-- LSP client, so they can be sent to the backend compiler one at a time, and a
-- reply sent.

data ReactorInput
  = HandlerRequest FromClientMessage
    -- ^ injected into the reactor input by each of the individual
    -- callback handlers

data State = State {stateCounter :: Int, stateProgram :: Maybe Imports}
initialState :: State
initialState = State 0 Nothing

-- | The monad used in the reactor
type R c a = StateT State (ReaderT (Core.LspFuncs c) IO) a
-- reactor monad functions
reactorSend :: FromServerMessage -> R () ()
reactorSend msg = do
  lf <- ask
  liftIO $ Core.sendFunc lf msg

publishDiagnostics :: Int -> J.NormalizedUri -> J.TextDocumentVersion -> DiagnosticsBySource -> R () ()
publishDiagnostics maxToPublish uri v diags = do
  -- lf should be set to a function that does IO (when publishing diagnostics)
  lf <- ask
  liftIO $ (Core.publishDiagnosticsFunc lf) maxToPublish uri v diags

nextLspReqId :: R () J.LspId
nextLspReqId = do
  f <- asks Core.getNextReqId
  liftIO f

-- | The single point that all events flow through, allowing management of state
-- to stitch replies and requests together from the two asynchronous sides: lsp
-- server and backend compiler
reactor :: Core.LspFuncs () -> TChan ReactorInput -> IO ()
reactor lf inp = do
  liftIO $ U.logs "reactor:entered"
  flip runReaderT lf $ flip evalStateT initialState $ forever $ do
    inval <- liftIO $ atomically $ readTChan inp
    case inval of

      -- Handle any response from a message originating at the server, such as
      -- "workspace/applyEdit"
      HandlerRequest (RspFromClient rm) -> do
        liftIO $ U.logs $ "reactor:got RspFromClient:" ++ show rm

      -- -------------------------------

      HandlerRequest (NotInitialized _notification) -> do
        liftIO $ U.logm "****** reactor: processing Initialized Notification"
        -- Server is ready, register any specific capabilities we need

         {-
         Example:
         {
                 "method": "client/registerCapability",
                 "params": {
                         "registrations": [
                                 {
                                         "id": "79eee87c-c409-4664-8102-e03263673f6f",
                                         "method": "textDocument/willSaveWaitUntil",
                                         "registerOptions": {
                                                 "documentSelector": [
                                                         { "language": "javascript" }
                                                 ]
                                         }
                                 }
                         ]
                 }
         }
        -}
        let
          registration = J.Registration "futhark lsp registered" J.WorkspaceExecuteCommand Nothing
        let registrations = J.RegistrationParams (J.List [registration])
        rid <- nextLspReqId

        reactorSend $ ReqRegisterCapability $ fmServerRegisterCapabilityRequest rid registrations

        -- example of showMessageRequest
        let
          params = J.ShowMessageRequestParams J.MtWarning "choose an option for XXX"
                           (Just [J.MessageActionItem "option a", J.MessageActionItem "option b"])
        rid1 <- nextLspReqId

        reactorSend $ ReqShowMessage $ fmServerShowMessageRequest rid1 params

      -- -------------------------------

      HandlerRequest (NotDidOpenTextDocument notification) -> do
        liftIO $ U.logm "****** reactor: processing NotDidOpenTextDocument"
        let
            doc  = notification ^. J.params
                                 . J.textDocument
                                 . J.uri
            fileName =  J.uriToFilePath doc
        liftIO $ U.logs $ "********* fileName=" ++ show fileName
        sendDiagnostics (J.toNormalizedUri doc) (Just 0)
        lf <- ask
        liftIO $ getAndPublishStatus (J.toNormalizedUri doc) (Just 0) fileName lf

      -- -------------------------------

      HandlerRequest (NotDidChangeTextDocument notification) -> do
        let doc :: J.NormalizedUri
            doc  = notification ^. J.params
                                 . J.textDocument
                                 . J.uri
                                 . to J.toNormalizedUri
        mdoc <- liftIO $ Core.getVirtualFileFunc lf doc
        case mdoc of
          Just (VirtualFile _version str) -> do
            liftIO $ U.logs $ "reactor:processing NotDidChangeTextDocument: vf got:" ++ (show $ Rope.toString str)
          Nothing -> do
            liftIO $ U.logs "reactor:processing NotDidChangeTextDocument: vf returned Nothing"

        liftIO $ U.logs $ "reactor:processing NotDidChangeTextDocument: uri=" ++ (show doc)

      -- -------------------------------

      HandlerRequest (NotDidSaveTextDocument notification) -> do
        liftIO $ U.logm "****** reactor: processing NotDidSaveTextDocument"
        let
            doc  = notification ^. J.params
                                 . J.textDocument
                                 . J.uri
            fileName = J.uriToFilePath doc
        liftIO $ U.logs $ "********* fileName=" ++ show fileName
        --liftIO $ hPutStr stderr $ show fileName
        --liftIO $ dumpStatus fileName
        -- Here we should have a call to a function that
        -- gets warnings from the compiler and publishes them
        lf <- ask
        liftIO $ getAndPublishStatus (J.toNormalizedUri doc) Nothing fileName lf
         

      -- -------------------------------

      HandlerRequest (ReqRename req) -> do
        liftIO $ U.logs $ "reactor:got RenameRequest:" ++ show req
        let
            _params = req ^. J.params
            _doc  = _params ^. J.textDocument . J.uri
            J.Position _l _c' = _params ^. J.position
            _newName  = _params ^. J.newName

        let we = J.WorkspaceEdit
                    Nothing -- "changes" field is deprecated
                    (Just (J.List [])) -- populate with actual changes from the rename
        let rspMsg = Core.makeResponseMessage req we
        reactorSend $ RspRename rspMsg

      -- -------------------------------

      HandlerRequest (ReqHover req) -> do
        liftIO $ U.logs $ "reactor:got HoverRequest:" ++ show req
        -- The third argument, Nothing, is an optional "_workDoneToken" 
        let J.TextDocumentPositionParams _doc pos Nothing = req ^. J.params
            J.Position _l _c' = pos
            J.TextDocumentIdentifier _uri = _doc
        --liftIO $ dump $ show $  J.uriToFilePath _uri

        do
          lf <- ask
          liftIO $ getHoverInfo (J.uriToFilePath _uri) _l _c' req lf
        -- let
        --   ht = Just $ J.Hover ms (Just range)
        --   -- We assume ms is short for message or the like
        --   -- We need to use <> for string concatenation, since T.Text values are not strings
        --   -- T.pack has type string -> T.Text and can be used to convert a string
        --   -- show can convert ints to strings
        --   -- Instead of using the IO monad to print values, we can use Trace.Debug. traceShowId is (almost) equivalent to show. 
        --   -- It calls show on our value, prints it as a sideffect, and passes that value on
        --   ms = J.HoverContents $ J.markedUpContent ("Line: " <> T.pack (show $ _l + 1) <> "\tColumn: " <> T.pack (show $ _c') <> "\nlsp-hover-request") "TYPE INFO GOES HERE"   
        --   range = J.Range pos pos
          -- Builds the response and sends it back to the client.
          -- traceShowId ht will debug-print all messages to stderr

        -- dump compile status to stderr


        -- get info at point, make it T.Text and use the reactor to fire it into the atmosphere (of visual studio code, the deadest place of all)
--        reactorSend $ RspHover $ Core.makeResponseMessage req $ traceShowId ht


      -- -------------------------------

      HandlerRequest (ReqCodeAction req) -> do
        liftIO $ U.logs $ "reactor:got CodeActionRequest:" ++ show req
        let params = req ^. J.params
            doc = params ^. J.textDocument
            -- fileName = drop (length ("file://"::String)) doc
            -- J.Range from to = J._range (params :: J.CodeActionParams)
            (J.List diags) = params ^. J.context . J.diagnostics

        let
          -- makeCommand only generates commands for diagnostics whose source is us
          makeCommand (J.Diagnostic (J.Range start _) _s _c (Just "lsp-hello") _m _l) = [J.Command title cmd cmdparams]
            where
              title = "Apply LSP hello command:" <> head (T.lines _m)
              -- NOTE: the cmd needs to be registered via the InitializeResponse message. See lspOptions above
              cmd = "lsp-hello-command"
              -- need 'file' and 'start_pos'
              args = J.List
                      [ J.Object $ H.fromList [("file",     J.Object $ H.fromList [("textDocument",J.toJSON doc)])]
                      , J.Object $ H.fromList [("start_pos",J.Object $ H.fromList [("position",    J.toJSON start)])]
                      ]
              cmdparams = Just args
          makeCommand (J.Diagnostic _r _s _c _source _m _l) = []
        let body = J.List $ map J.CACommand $ concatMap makeCommand diags
            rsp = Core.makeResponseMessage req body
        reactorSend $ RspCodeAction rsp

      -- -------------------------------

      HandlerRequest (ReqExecuteCommand req) -> do
        liftIO $ U.logs "reactor:got ExecuteCommandRequest:" -- ++ show req
        let params = req ^. J.params
            margs = params ^. J.arguments

        liftIO $ U.logs $ "reactor:ExecuteCommandRequest:margs=" ++ show margs

        let
          reply v = reactorSend $ RspExecuteCommand $ Core.makeResponseMessage req v
        -- When we get a RefactorResult or HieDiff, we need to send a
        -- separate WorkspaceEdit Notification
          r = J.List [] :: J.List Int
        liftIO $ U.logs $ "ExecuteCommand response got:r=" ++ show r
        case toWorkspaceEdit r of
          Just we -> do
            reply (J.Object mempty)
            lid <- nextLspReqId
            -- reactorSend $ J.RequestMessage "2.0" lid "workspace/applyEdit" (Just we)
            reactorSend $ ReqApplyWorkspaceEdit $ fmServerApplyWorkspaceEditRequest lid we
          Nothing ->
            reply (J.Object mempty)

      -- -------------------------------
      
      HandlerRequest (ReqDefinition req) -> do
        liftIO $ U.logs "reactor:got GotoDefinitionRequest:"
        -- The third argument, Nothing, is an optional "_workDoneToken" 
        let J.TextDocumentPositionParams _doc pos Nothing = req ^. J.params
            J.Position _l _c' = pos
            J.TextDocumentIdentifier _uri = _doc
            --reply v = reactorSend $ RspDefinition $ Core.makeResponseMessage req v
            --test = (J.Location (J.fromNormalizedUri $ J.toNormalizedUri _doc) (J.Range (J.Position 0 0) (J.Position 0 0)))
        --liftIO $ dump $ show $  J.uriToFilePath _uri
        --reply test
        return ()
        --response:
        --         J.Position start = 
        --         J.Position end   = 
        --         J.Location (J.toNormalizedUri doc) (J.Range start end)
      -- -------------------------------

      HandlerRequest om -> do
        liftIO $ U.logs $ "reactor:got HandlerRequest:" ++ show om

toWorkspaceEdit :: t -> Maybe J.ApplyWorkspaceEditParams
toWorkspaceEdit _ = Nothing

  
-- sendFutharkDiagnostics :: J.NormalizedUri -> Maybe Int -> [J.Diagnostic] -> R () ()
-- sendFutharkDiagnostics fileUri version diags = do
--   -- I am pretty sure lf is the servermethod, e.g. WindowShowMessage, WindowLogMessage, workspaceApplyEdit etc.
--   -- it is injected into Core.publishDiagnosticsFunc, creating a new function
--   -- that does IO and sends a message back to the server
--   lf <- ask
--   -- lift into IO, so we can get the warnings from the compiler and send them back
--   -- so we need to write a function that gets the warnings, creates diags and then has the line below
--   -- ideally, liftIO $ sendWarnings lf 100 fileUri version 
-- --  lifIO $ (Core.publishDiagnosticsFunc lf) 100 fileUri version (partitionBySource diags)
-- --  publishDiagnostics 100 fileUri version (partitionBySource diags)


-- | Analyze the file and send any diagnostics to the client in a
-- "textDocument/publishDiagnostics" notification
sendDiagnostics :: J.NormalizedUri -> Maybe Int -> R () ()
sendDiagnostics fileUri version = do
  let
    diags = [J.Diagnostic
              (J.Range (J.Position 0 1) (J.Position 0 5))
              (Just J.DsWarning)  -- severity
              Nothing  -- code
              (Just "lsp-hello") -- source
              "Example diagnostic message"
              (Just (J.List []))
            ]
  -- reactorSend $ J.NotificationMessage "2.0" "textDocument/publishDiagnostics" (Just r)
  publishDiagnostics 100 fileUri version (partitionBySource diags)

getStatus :: Maybe String -> IO T.Text
getStatus file =
    case file of
    Nothing -> return "" --()
    Just filename -> do
      res <- runFutharkM (readProgram filename) NotVerbose
      case res of
        Left _ -> return $ T.pack $ "Failed to compile " ++ filename ++ "\n"
        Right (w,_,_) -> return $ T.pack $ show w


getHoverInfo :: Maybe FilePath -> Int -> Int -> J.HoverRequest -> Core.LspFuncs () -> IO ()
getHoverInfo filename line col req lf = do
  case filename of
    Nothing -> dump "No file was supplied\n"
    Just filename -> do
      res <- runFutharkM (readProgram filename) NotVerbose
      case res of
        Left _ -> do dump "compilation failed on hover request.\n"
        Right (_,imports,_) -> do
          case atPos imports $ Pos filename (line+1) (col+1) 0 of
            --      case atPos imports pos of
            Nothing -> return ()-- dump "No information available\n"
            Just (AtName qn def loc) -> do
              -- dump $ "Name: " ++ pretty qn ++ "\n"
              -- dump $ "Position: " ++ locStr loc ++ "\n"
              case def of
                Nothing -> return ()
                Just (BoundTerm t defloc) -> do
                  -- dump $ "Type: " ++ pretty t ++ "\n"
                  -- dump $ "Definition: " ++ locStr (srclocOf defloc) ++ "\n"
                  let
                    ht = Just $ J.Hover ms (Just range)
                    ms = J.HoverContents $ J.markedUpContent  ""
                        (T.pack ( pretty qn ++ " :: " ++ pretty t ++ "\n" ++
                        --"Position: " ++ locStr loc ++ "\n" ++
                        "Defined at: " ++locStr (srclocOf defloc) ++ "\n")) 
                    range = J.Range (J.Position line col) (J.Position line col) 

                  Core.sendFunc lf $ RspHover $ Core.makeResponseMessage req $ traceShowId ht
--                  return ()
 

getAndPublishStatus :: J.NormalizedUri -> Maybe Int -> Maybe String -> Core.LspFuncs () -> IO ()
getAndPublishStatus uri version fileName lf =
  do
    dump "entered getAndPublishStatus"
    dump $ "Version: " ++ show version
    -- dump the filename
    case fileName of
      Nothing -> dump "No file was supplied.."
      Just filename -> do 
        --dump $ "filename: " ++ filename
        res <- runFutharkM (readProgram filename) NotVerbose
        case res of
          Left e -> do
              -- /:(\d+):(\d+)-(\d+): ?\n? +(.+)/
              -- group1 + group2 = startPos
              -- group1 + group3 = endPos
              -- group4          = message
            let regex = mkRegexWithOpts ":([0-9]+):([0-9]+)-([0-9]+):[ |\n +](.+)" True False
            case matchRegexAll regex $ show e of
              Nothing -> dump "Something went wrong while checking for errors"
              Just (_,_,_,strs) -> do
                dump $ show $ length strs
                let diags =
                      [J.Diagnostic
                        (J.Range (J.Position ((read $ strs!!0 :: Int) - 1) ((read $ strs!!1 :: Int) - 1)) (J.Position ((read $ strs!!0 :: Int) - 1) (read $ strs!!2 :: Int)))
                        (Just J.DsError)  -- severity
                        Nothing  -- code
                        (Just $ T.pack $ show e) -- source
                        (T.pack $ strs!!3) -- Convert the warnings to T.text and put them in diags
                        (Just (J.List []))
                      ]
                (Core.publishDiagnosticsFunc lf) 100 uri version (partitionBySource diags)
          Right (w,_,_) -> do
            let regex = mkRegexWithOpts ":([0-9]+):([0-9]+)-([0-9]+):[ |\n +](.+)" True False
            case matchRegexAll regex $ show w of
              Nothing -> dump "Something went wrong while checking for warnings"
              Just (_,_,_,strs) -> do
                dump $ show $ length strs
                let diags =
                      [J.Diagnostic
                        (J.Range (J.Position ((read $ strs!!0 :: Int) - 1) ((read $ strs!!1 :: Int) - 1)) (J.Position ((read $ strs!!0 :: Int) - 1) (read $ strs!!2 :: Int)))
                        (Just J.DsWarning)  -- severity
                        Nothing  -- code
                        (Just $ T.pack $ show w) -- source
                        (T.pack $ strs!!3) -- Convert the warnings to T.text and put them in diags
                        (Just (J.List []))
                      ]
                -- * lf is the LSP server Method
                -- * (Core.publishDiagnosticsFunc lf) gives us a function that can 
                --   send diagnostics to the client
                -- * 100 is max_warnings. TODO Replace with variable
                (Core.publishDiagnosticsFunc lf) 100 uri version (partitionBySource diags)
    

syncOptions :: J.TextDocumentSyncOptions
syncOptions = J.TextDocumentSyncOptions
  { J._openClose         = Just True
  , J._change            = Just J.TdSyncIncremental
  , J._willSave          = Just False
  , J._willSaveWaitUntil = Just False
  , J._save              = Just $ J.SaveOptions $ Just False
  }

lspOptions :: Core.Options
lspOptions = def { Core.textDocumentSync = Just syncOptions
-- deprecated (?)                , Core.executeCommandProvider = Just (J.ExecuteCommandOptions (J.List ["lsp-hello-command"]))
                 }

lspHandlers :: TChan ReactorInput -> Core.Handlers
lspHandlers rin =
  def { Core.initializedHandler                       = Just $ passHandler rin NotInitialized
      , Core.renameHandler                            = Just $ passHandler rin ReqRename
      , Core.hoverHandler                             = Just $ passHandler rin ReqHover
      , Core.didOpenTextDocumentNotificationHandler   = Just $ passHandler rin NotDidOpenTextDocument
      , Core.didSaveTextDocumentNotificationHandler   = Just $ passHandler rin NotDidSaveTextDocument
      , Core.didChangeTextDocumentNotificationHandler = Just $ passHandler rin NotDidChangeTextDocument
      , Core.didCloseTextDocumentNotificationHandler  = Just $ passHandler rin NotDidCloseTextDocument
      , Core.cancelNotificationHandler                = Just $ passHandler rin NotCancelRequestFromClient
      , Core.responseHandler                          = Just $ responseHandlerCb rin
      , Core.codeActionHandler                        = Just $ passHandler rin ReqCodeAction
      , Core.definitionHandler                        = Just $ passHandler rin ReqDefinition
-- deprecated (?) , Core.executeCommandHandler                    = Just $ passHandler rin ReqExecuteCommand
      }

passHandler :: TChan ReactorInput -> (a -> FromClientMessage) -> Core.Handler a
passHandler rin c notification = do
  atomically $ writeTChan rin (HandlerRequest (c notification))

responseHandlerCb :: TChan ReactorInput -> Core.Handler J.BareResponseMessage
responseHandlerCb _rin resp = do
  U.logs $ "******** got ResponseMessage, ignoring:" ++ show resp
