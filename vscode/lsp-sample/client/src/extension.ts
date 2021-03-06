/* --------------------------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for license information.
 * ------------------------------------------------------------------------------------------ */

import * as vscode from 'vscode';
import * as languageClient from 'vscode-languageclient';
import * as path from 'path';

// Futhark installs to /.local/bin by default (with 'stack install')
const languageServerPath = path.join(require('os').homedir(), "/.local/bin/");

function activateLanguageServer(context: vscode.ExtensionContext) {
	let serverModule: string = languageServerPath;
	
	if(!serverModule) throw new URIError("Cannot find the language server module.");
	let workPath = path.dirname(serverModule);
	console.log(`Use ${serverModule} as server module.`);
	console.log(`Work path: ${workPath}.`);
	
	// If the extension is launched in debug mode then the debug server options are used
	// Otherwise the run options are used
	// Also there are no debug options, so it is irelevant
	let serverOptions: languageClient.ServerOptions = {
		run: { command: "futhark", args: ["lsp"], options: { cwd: workPath} },
		debug: { command: "futhark", args: ["lsp"], options: { cwd: workPath} }
	};

	// Options to control the language client
	let clientOptions: languageClient.LanguageClientOptions = {
		// Register the server for plain text documents
		// TODO: Futhark language extension
		documentSelector: [{ scheme: 'file', language: 'plaintext' }],
		synchronize: {
			// Notify the server about file changes to '.clientrc files contained in the workspace
			fileEvents:vscode.workspace.createFileSystemWatcher('**/.clientrc')
		}
	};

	// Create the language client and start the client.
	let client = new languageClient.LanguageClient(
		'futharkServerTest',
		'Language Server Example',
		serverOptions,
		clientOptions
	);

	let disposable = client.start();
	context.subscriptions.push(disposable);
}

export function activate(context: vscode.ExtensionContext) {
	console.log("futhark test server is activated!");
	// Activate the server
	activateLanguageServer(context);

	// Register a test-command
	let disposable = vscode.commands.registerCommand("extension.sayHello", () => {
		vscode.window.showInformationMessage("Hello, World!");
	});

	context.subscriptions.push(disposable);
}

export function deactivate() {
}
