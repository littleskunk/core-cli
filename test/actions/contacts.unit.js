'use strict';
var expect = require('chai').expect;
var proxyquire = require('proxyquire');
var sinon = require('sinon');

var LoggerStub = {
  log: sinon.stub()
};
var storjStub = {};

var Contacts = proxyquire('../../bin/actions/contacts.js', {
  './../logger': function() {
    return LoggerStub;
  },
  'storj-lib': storjStub
});

describe('contacts', function() {
  beforeEach(function() {
    LoggerStub.log.reset();
    for (var k in storjStub) {
      try {
        delete storjStub[k];
      } catch(e) {
        // occurs when a key in the object is not writable
      }
    }
  });

  describe('#list', function() {
    it('should log an error if the client returns one', function() {
      var error = {
        message: 'This is an error.'
      };
      var PublicClientStub = {
        getContactList: function(info, cb) {
          cb(error);
        }
      };
      Contacts._storj = {
        PublicClient: sinon.stub().returns(PublicClientStub)
      };

      Contacts.list(1);

      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('error',
        error.message)).to.equal(true);
    });

    it('should log a warning if there are no contacts', function() {
      var PublicClientStub = {
        getContactList: function(info, cb) {
          cb(null, []);
        }
      };
      Contacts._storj = {
        PublicClient: sinon.stub().returns(PublicClientStub)
      };

      Contacts.list(1);

      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('warn',
        'There are no contacts to show')).to.equal(true);
    });

    it('should log info about each contact that is returned', function() {
      var contactList = [
        {name: 'testname1', lastSeen: 20, protocol: 'testprotocol1'},
        {name: 'testname2', lastSeen: 52},
        {name: 'testname3', lastSeen: 11, protocol: 'testprotocol2'}
      ];
      var PublicClientStub = {
        getContactList: function(info, cb) {
          cb(null, contactList);
        }
      };
      Contacts._storj = {
        PublicClient: sinon.stub().returns(PublicClientStub)
      };
      storjStub.utils = {
        getContactURL: function(contact) {
          return contact.name;
        }
      };

      Contacts.list(1);

      expect(LoggerStub.log.callCount).to.equal(4 * contactList.length);
      contactList.forEach(function(contact) {
        var protocol = contact.protocol || '?';
        expect(LoggerStub.log.calledWithMatch('info', 'Contact:   ' +
          contact.name)).to.equal(true);
        expect(LoggerStub.log.calledWithMatch('info', 'Last Seen: ' +
          contact.lastSeen)).to.equal(true);
        expect(LoggerStub.log.calledWithMatch('info', 'Protocol:  ' +
          protocol)).to.equal(true);
      });
    });
  });

  describe('#get', function() {
    it('should log an error if the client returns one', function() {
      var error = {
        message: 'This is an error.'
      };
      var PublicClientStub = {
        getContactByNodeId: function(id, cb) {
          cb(error);
        }
      };
      Contacts._storj = {
        PublicClient: sinon.stub().returns(PublicClientStub)
      };

      Contacts.get(1);

      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('error',
        error.message)).to.equal(true);
    });

    it('should log info about the contact if there is no error', function() {
      var contactList = [
        {name: 'testname1', lastSeen: 20, protocol: 'testprotocol1'},
        {name: 'testname2', lastSeen: 52},
        {name: 'testname3', lastSeen: 11, protocol: 'testprotocol2'}
      ];
      var PublicClientStub = {
        getContactByNodeId: function(id, cb) {
          cb(null, contactList[id]);
        }
      };
      Contacts._storj = {
        PublicClient: sinon.stub().returns(PublicClientStub)
      };
      storjStub.utils = {
        getContactURL: function(contact) {
          return contact.name;
        }
      };

      Contacts.get(1);

      var contact = contactList[1];
      expect(LoggerStub.log.callCount).to.equal(3);
      var protocol = contact.protocol || '?';
      expect(LoggerStub.log.calledWithMatch('info', 'Contact',
        [contact.name])).to.equal(true);
      expect(LoggerStub.log.calledWithMatch('info', 'Last Seen',
        [contact.lastSeen])).to.equal(true);
      expect(LoggerStub.log.calledWithMatch('info', 'Protocol',
        [protocol])).to.equal(true);
    });
  });
});
