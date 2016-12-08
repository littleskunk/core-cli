'use strict';
var log = require('./../logger')().log;
var utils = require('./../utils');
var prompt = require('prompt');

module.exports.generateSeed = function() {

  var keypass = this._storj.getKeyPass();
  utils.getKeyRing(keypass, function(keyring) {
    try {
      keyring.generateDeterministicKey();
      log('info', 'Seed successfully generated');
    } catch(err) {
      log('error', err.message);
    }
  });

};

module.exports.printSeed = function() {
  var keypass = this._storj.getKeyPass();
  utils.getKeyRing(keypass, function(keyring) {
    var mnemonic = keyring.exportMnemonic();
    if (!mnemonic) {
      log('info', 'Seed has not been generated or imported');
    } else {
      log('info', mnemonic);
    }
  });
};

module.exports.importSeed = function() {
  var keypass = this._storj.getKeyPass();
  utils.getKeyRing(keypass, function(keyring) {

    if (keyring.exportMnemonic()) {
      return log('error', 'Mnemonic already exists');
    }

    prompt.start();
    prompt.get({
      properties: {
        mnemonic: {
          description: 'Please enter mnemonic:',
          required: true
        }
      }
    }, function(err, result) {
      try {
        keyring.importMnemonic(result.mnemonic);
        log('info', 'Mnemonic successfully imported');
      } catch(err) {
        log('error', err.message);
      }
    });

  });
};

module.exports.deleteSeed = function() {
  var keypass = this._storj.getKeyPass();
  utils.getKeyRing(keypass, function(keyring) {
    keyring.deleteDeterministicKey();
    log('info', 'Mnemonic successfully deleted');
  });
};
