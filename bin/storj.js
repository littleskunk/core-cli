#!/usr/bin/env node

'use strict';

var program = require('commander');
var fs = require('fs');
var os = require('os');
var platform = os.platform();
var path = require('path');
var prompt = require('prompt');
var colors = require('colors/safe');
var storj = require('storj-lib');
var merge = require('merge');
var logger = require('./logger');
var log = logger().log;
var utils = require('./utils');
var actions = require('./index');
var url = require('url');

var HOME = platform !== 'win32' ? process.env.HOME : process.env.USERPROFILE;
var DATADIR = path.join(HOME, '.storjcli');

if (!storj.utils.existsSync(DATADIR)) {
  fs.mkdirSync(DATADIR);
}

prompt.message = colors.bold.cyan(' [...]');
prompt.delimiter = colors.cyan('  > ');
program._storj = {};

program.version(
  'Storjcli: ' + require('../package').version + ' | ' +
  'Core: ' + storj.version.software
);
program.option('-u, --url <url>', 'set the base url for the api');
program.option('-n, --byname', 'treat ids as names');
program.option('-k, --keypass <password>', 'unlock keyring without prompt');
program.option('-d, --debug', 'display debug data', 4);

program._storj.loglevel = function() {
  return program.debug || 3;
};

program._storj.getURL = function() {
  return program.url || process.env.STORJ_BRIDGE || 'https://api.storj.io';
};

program._storj.path = function(prefix) {
  var file = prefix + url.parse(program._storj.getURL()).hostname +')';
  return path.join(DATADIR, file);
};

program._storj.keypath = program._storj.path('id_ecdsa_(');
program._storj.idpath = program._storj.path('id_user_(');

program._storj.PrivateClient = function(options) {
  if (typeof options === 'undefined') {
    options = {};
  }

  options.blacklistFolder = DATADIR;
  options.requestTimeout = options.requestTimeout || 10000;

  return storj.BridgeClient(program._storj.getURL(), merge({
    keyPair: program._storj.loadKeyPair(),
    logger: logger(program._storj.loglevel()).log
  }, options));
};

program._storj.PublicClient = function() {
  return storj.BridgeClient(
    program._storj.getURL(),
    { logger: logger(program._storj.loglevel()).log }
  );
};

program._storj.getKeyPass = function() {
  return program.keypass || process.env.STORJ_KEYPASS || null;
};

program._storj.loadKeyPair = function(){
  if (!storj.utils.existsSync(program._storj.keypath)) {
    log('error', 'You have not authenticated, please login.');
    process.exit(1);
  }

  return storj.KeyPair(fs.readFileSync(program._storj.keypath).toString());
};

/**
  * Calculate real bucket id from either bucket name or id
  * @param {String} bucketArg - Bucket name or bucket id
  * @param {String} [userId] - override your own id
  */
program._storj.getRealBucketId = function(bucketArg, userId){
  // return bucketArg if we don't have a userId
  if (!storj.utils.existsSync(program._storj.idpath) && !userId) {
    return bucketArg;
  }
  // retrieve our own id if one was not passed in
  if (!userId) {
    userId = fs.readFileSync(program._storj.idpath).toString();
  }
  // translate to name if argument doesn't match id or resolution is forced
  if (!bucketArg.match(/^[0-9a-f]{24}$/i) || program.byname) {
    return storj.utils.calculateBucketId(userId, bucketArg);
  }
  return bucketArg;
};

/**
  * Calculate real file id from either file name or id
  * @param {String} bucketId - Bucket id
  * @param {String} fileArg - file name or file id
  */
program._storj.getRealFileId = function(bucketId, fileArg){
  if(!fileArg.match(/^[0-9a-f]{24}$/i) || program.byname){
    return storj.utils.calculateFileId(bucketId, fileArg);
  }
  return fileArg;
};

var ACTIONS = {
  fallthrough: function(command) {
    log(
      'error',
      'Unknown command "%s", please use --help for assistance',
      command
    );
    program.help();
  },
  upload: function(bucket, filepath, env) {
    bucket = program._storj.getRealBucketId(bucket, env.user);
    var options = {
      filepath: filepath,
      keypass: program._storj.getKeyPass(),
      env: env
    };
    var uploader;

    try {
      uploader = new actions.Uploader(
        program._storj.PrivateClient,
        bucket,
        options
      );
    } catch(err) {
      return log('error', err.message);
    }

    uploader.start(function(err) {
      if (err) {
        log('error', err.message);
        process.exit(1);
      }
    });
  },
  download: function(bucket, id, filepath, env) {
    bucket = program._storj.getRealBucketId(bucket, env.user);
    id = program._storj.getRealFileId(bucket, id);
    var options = {
      filepath: filepath,
      keypass: program._storj.getKeyPass(),
      env: env
    };
    var downloader;

    try {
      downloader = new actions.Downloader(
        program._storj.PrivateClient,
        id,
        bucket,
        options
      );
    } catch(err) {
      return log('error', err.message);
    }

    downloader.start(function(err) {
      if (err) {
        log('error', err.message);
        process.exit(1);
      }
    });
  }
};

program
  .command('get-info')
  .description('get remote api information')
  .action(actions.account.getInfo.bind(program));

program
  .command('register')
  .description('register a new account with the storj api')
  .action(actions.account.register.bind(program));

program
  .command('login')
  .description('authorize this device to access your storj api account')
  .action(actions.account.login.bind(program));

program
  .command('logout')
  .description('revoke this device\'s access your storj api account')
  .action(actions.account.logout.bind(program));

program
  .command('reset-password <email>')
  .description('request an account password reset email')
  .action(actions.account.resetpassword.bind(program));

program
  .command('list-keys')
  .description('list your registered public keys')
  .action(actions.keys.list.bind(program));

program
  .command('add-key <pubkey>')
  .description('register the given public key')
  .action(actions.keys.add.bind(program));

program
  .command('remove-key <pubkey>')
  .option('-f, --force', 'skip confirmation prompt')
  .description('invalidates the registered public key')
  .action(actions.keys.remove.bind(program));

program
  .command('list-buckets')
  .description('list your storage buckets')
  .action(actions.buckets.list.bind(program));

program
  .command('get-bucket <bucket-id>')
  .description('get specific storage bucket information')
  .action(actions.buckets.get.bind(program));

program
  .command('add-bucket [name] [storage] [transfer]')
  .description('create a new storage bucket')
  .action(actions.buckets.add.bind(program));

program
  .command('remove-bucket <bucket-id>')
  .option('-f, --force', 'skip confirmation prompt')
  .description('destroys a specific storage bucket')
  .action(actions.buckets.remove.bind(program));

program
  .command('update-bucket <bucket-id> [name] [storage] [transfer]')
  .description('updates a specific storage bucket')
  .action(actions.buckets.update.bind(program));

program
  .command('make-public <bucket-id>')
  .option('--pull', 'make PULL operations public')
  .option('--push', 'make PUSH operations public')
  .description('makes a specific storage bucket public, ' +
    'uploading bucket key to bridge')
  .action(actions.buckets.makePublic.bind(program));

program
  .command('add-frame')
  .description('creates a new file staging frame')
  .action(actions.frames.add.bind(program));

program
  .command('list-frames')
  .description('lists your file staging frames')
  .action(actions.frames.list.bind(program));

program
  .command('get-frame <frame-id>')
  .description('retreives the file staging frame by id')
  .action(actions.frames.get.bind(program));

program
  .command('remove-frame <frame-id>')
  .option('-f, --force', 'skip confirmation prompt')
  .description('removes the file staging frame by id')
  .action(actions.frames.remove.bind(program));

program
  .command('export-keyring <directory>')
  .description('compresses and exports keyring to specific directory')
  .action(utils.exportkeyring.bind(program));

program
  .command('import-keyring <path>')
  .description('imports keyring tarball into current keyring')
  .action(utils.importkeyring.bind(program));

program
  .command('get-file-info <bucket-id> <file-id>')
  .description('gets information about a file')
  .action(actions.files.getInfo.bind(program));

program
  .command('list-files <bucket-id>')
  .description('list the files in a specific storage bucket')
  .action(actions.files.list.bind(program));

program
  .command('list-mirrors <bucket-id> <file-id>')
  .description('list the mirrors for a given file')
  .action(actions.files.listMirrors.bind(program));

program
  .command('remove-file <bucket-id> <file-id>')
  .option('-f, --force', 'skip confirmation prompt')
  .description('delete a file pointer from a specific bucket')
  .action(actions.files.remove.bind(program));

program
  .command('upload-file <bucket-id> <file-glob>')
  .option('-c, --concurrency <count>', 'max shard upload concurrency')
  .option('-C, --fileconcurrency <count>', 'max file upload concurrency', 1)
  .option('-u, --user <user>', 'user id for public name resolution', null)
  .description('upload a file or files to the network and track in a bucket')
  .action(ACTIONS.upload);

program
  .command('create-mirrors <bucket-id> <file-id>')
  .option('-r, --redundancy [mirrors]', 'mirrors to create for file', 3)
  .description('create redundant mirrors for the given file')
  .action(actions.files.mirror.bind(program));

program
  .command('download-file <bucket-id> <file-id> <filepath>')
  .option('-x, --exclude <nodeID,nodeID...>', 'mirrors to create for file', '')
  .option('-u, --user <user>', 'user id for public name resolution', null)
  .description('download a file from the network with a pointer from a bucket')
  .action(ACTIONS.download);

program
  .command('generate-key')
  .option('-s, --save <path>', 'save the generated private key')
  .option('-e, --encrypt <passphrase>', 'encrypt the generated private key')
  .description('generate a new ecdsa key pair and print it')
  .action(utils.generatekey.bind(program));

program
  .command('get-contact <nodeid>')
  .description('get the contact information for a given node id')
  .action(actions.contacts.get.bind(program));

program
  .command('get-pointers <bucket-id> <file-id>')
  .option('-s, --skip <index>', 'starting index for file slice', 0)
  .option('-n, --limit <number>', 'total pointers to return from index', 6)
  .description('get pointers metadata for a file in a bucket')
  .action(actions.files.getpointers.bind(program));

program
  .command('create-token <bucket-id> <operation>')
  .description('create a push or pull token for a file')
  .action(actions.buckets.createtoken.bind(program));

program
  .command('list-contacts [page]')
  .option('-c, --connected', 'limit results to connected nodes')
  .description('list the peers known to the remote bridge')
  .action(actions.contacts.list.bind(program));

program
  .command('prepare-audits <total> <filepath>')
  .description('generates a series of challenges used to prove file possession')
  .action(utils.prepareaudits.bind(program));

program
  .command('prove-file <merkleleaves> <challenge> <filepath>')
  .description('generates a proof from the comma-delimited tree and challenge')
  .action(utils.provefile.bind(program));

program
  .command('change-keyring')
  .description('change the keyring password')
  .action(utils.changekeyring.bind(program));

  program
    .command('reset-keyring')
    .description('delete the current keyring and start a new one')
    .action(utils.resetkeyring.bind(program));

program
  .command('sign-message <privatekey> <message>')
  .option('-c, --compact', 'use bitcoin-style compact signature')
  .description('signs the message using the supplied private key')
  .action(utils.signmessage.bind(program));

program
  .command('stream-file <bucket-id> <file-id>')
  .option('-x, --exclude <nodeID,nodeID...>', 'mirrors to create for file', '')
  .description('stream a file from the network and write to stdout')
  .action(actions.files.stream.bind(program));

program
  .command('verify-proof <root> <depth> <proof>')
  .description('verifies the proof response given the merkle root and depth')
  .action(utils.verifyproof.bind(program));

program
  .command('test <bucket-id>')
  .description('get pointers metadata for all files in a bucket')
  .action(actions.files.getallpointers.bind(program));

program
  .command('generate-seed')
  .description('generates new deterministic seed')
  .action(actions.seed.generateSeed.bind(program));

program
  .command('print-seed')
  .description('prints the human readable deterministic seed')
  .action(actions.seed.printSeed.bind(program));

program
  .command('import-seed')
  .description('imports deterministic seed from another device')
  .action(actions.seed.importSeed.bind(program));

program
  .command('delete-seed')
  .description('deletes the deterministic seed from the keyring')
  .action(actions.seed.deleteSeed.bind(program));


program
  .command('*')
  .description('prints the usage information to the console')
  .action(ACTIONS.fallthrough);

// NB: If piping output to another program that does not consume all the output
// NB: (like `head`), don't throw an error.
process.stdout.on('error', function(err) {
  if (err.code === 'EPIPE') {
    process.exit(0);
  }
});

program.parse(process.argv);

// Awwwww <3
if (process.argv.length < 3) {
  return program.help();
}
