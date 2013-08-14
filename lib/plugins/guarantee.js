
/**
 * Module dependencies.
 */

var debug = require('debug')('axon:guarantee');

/**
 * Guarantee plugin.
 *
 * Provides `.commit()` and `.committed()` methods for `sock` (this).
 * Messages are passed to `commit` will be buffered until sent sock (rep)
 * replys with committed (have processed it), else if sock `disconnects` will
 * resend to another sock, hence guaranteeing delivery, `downstream`.
 *
 * Emits:
 *
 *  - `commit_drop` (msg) when a message is dropped
 *
 * @param {Object} options
 * @api private
 */
 
 
var util = require('util');
var RESEND_DELAY = 1000;
var COMMIT_TIMEOUT = 30000;

var LOGGING = 'quiet';
function _log(msg, dump) {
  if ( LOGGING === 'debug' && dump !== undefined ) {
    console.log('debug - ' + msg + ' > ' + require('util').inspect(dump, true, 99, true));
  } else if ( LOGGING === 'debug' && dump === undefined ) {
    console.log(msg);
  } else if ( LOGGING === 'alert' ) {
    console.log(msg);
  } else {  
  }
}


module.exports = function(options){
  options = options || {};

  return function(sock){


    sock.commits = {};  // object array containing each message waiting for commitment.



    // if disconnect event, check commits for any unprocessed messages and resend.
    sock.on('disconnect', function(_s){
      var downstream = _s._peername.address + ':' + _s._peername.port.toString();
      _log('downstream disconnect', downstream);
      var _sock = this;
      if ( _sock.commits[downstream] !== undefined ) {
        setTimeout(function(){
          resend(_sock, downstream);
        }, RESEND_DELAY);
      }
    });



		// commit Function.
    sock.commit = function(s, xmsg){

      //var _sock = this;
     
      var downstream = s._peername.address + ':' + s._peername.port.toString();

      _log('comit this message');
      var m = xmsg[0];  // get message out of args.
      var xmessages = xmsg[0].messages;
      
      var commits_hwm = sock.settings.commits_hwm;
      //_log('commits_hwm: ' + commits_hwm);
      var commits_size = Buffer.byteLength(JSON.stringify(sock.commits), 'utf8');
      //_log('commits_size: ' + commits_size);
      if ( commits_size >= commits_hwm ) {
        //_log('commits_hwm exceeded');
        return commit_drop(m);      
      }
      
      //_log('msg> ' + util.inspect(m, true, 99, true));

      // add messages to queue.
      if ( sock.commits[downstream] === undefined ) {
        sock.commits[downstream] = {};
      }
	
	    // PROB HERE WITH MESG OVERIDE FROM ARRAY TO OBJECT (sort-of-fixed with commitMsgArraytoObj fu).
	  
	    sock.commits[downstream][m.idb] = m;
	   /*
      
      delete sock.commits[downstream][m.idb]['messages'];
      sock.commits[downstream][m.idb].messages = {};

      // commits this message block, until downstream node has confirmed comitment.
      var x = 0;
      for ( x = 0; x < xmessages.length; x++ ) {
  	    sock.commits[downstream][m.idb]['messages'][(x + 1).toString()] = xmessages[x];
      }
     */
     
      (function (d,i) {
			  setTimeout(function () {
          _log('commit TimedOut: ' + d + '>' + i);
          if ( sock.settings.commits_timeout_resend ) {
          	_log('resend');
            resend(sock, d);
          } else {
            _log('delete');
            try {
              delete sock.commits[d][i];
              if ( Object.keys(sock.commits[d]).length === 0 ) {
                _log('nothing left in downstream _sock.commits so delete object from _sock.commits.');
                delete sock.commits[d];    	
              }                 
            } catch (e) {
            }
          }
          _log('commits', sock.commits);
		  	}, COMMIT_TIMEOUT);
		  })(downstream,m.idb);	
	   
 
      _log('commits', sock.commits);
      
      m['downstream'] = downstream;
      delete m['messages'];
      m['messages'] = xmessages;
      //_log('m> ' + util.inspect(m, true, 99, true));
      
      _log('FIN comit this message');
      return m;
      
    };


		// commitMsgArraytoObj Function.
    sock.commitMsgArraytoObj = function(msg){
      _log('commitMsgArraytoObj');
      _log('msg', msg);
      _log('commitFix',sock.commits);
      
      var m = msg[0];  // get message out of args.
      var xmessages = msg[0].messages;
      
      _log('m.downstream', m.downstream);
      _log('m.idb', m.idb);
      
      delete sock.commits[m.downstream][m.idb]['messages'];
      sock.commits[m.downstream][m.idb].messages = {};
      var x = 0;
      for ( x = 0; x < xmessages.length; x++ ) {
  	    sock.commits[m.downstream][m.idb]['messages'][(x + 1).toString()] = xmessages[x];
      }      
      _log('commitFix', sock.commits);
    };    
    
    
    /**
     * Drop the given `msg`.
     */

    function commit_drop(msg) {
      debug('commit_drop');
      //_log('commit_drop-ed');
      sock.emit('commit_drop', msg);
    }      
    
  
    // committed Function.
    sock.committed = function(msg){

      
     
      var m = msg[0];  // get message out of args.
      _log('committed this msg');
      _log('msg', m);
      
      //var _sock = this;

      
      _log('_sock.commitsA', sock.commits);
      

      var downstream = m.downstream;
      //_log('downstream: ' + downstream);
      
      if ( downstream !== undefined ) {
      	
        _log('_sock.commits2', sock.commits[downstream]);

        if ( sock.commits[downstream] !== undefined ) {
          _log('YES remove from q');
          try {
          
            var x = 0;
            var y = Object.keys(sock.commits[downstream][m.idb]['messages']).length;
            // go through each message id returned by downstream and delete from queue.
            for ( x = 0; x < y; x++ ) {  
              var mx = m.messages[x];
              _log('message to remove from _sock.commits', mx);
              if ( sock.commits[downstream][m.idb]['messages'][mx] !== undefined ) {
                _log('Yes m in q delete');
                delete sock.commits[downstream][m.idb]['messages'][mx];
              }
            }


            if ( Object.keys(sock.commits[downstream][m.idb]['messages']).length === 0 ) {
              _log('no message left in _sock.commits, so can delete idb from _sock.commits.');
              delete sock.commits[downstream][m.idb];	
            }
    
          } catch (e) {
            _log('commited error, ' + e.message);
          }    
    
 
          if ( Object.keys(sock.commits[downstream]).length === 0 ) {
            _log('nothing left in downstream _sock.commits so delete object from _sock.commits.');
            delete sock.commits[downstream];    	
          }   

          
        }

      }

      _log('_sock.commits3', sock.commits);      
      
    };   
    
   
  };
};



// resend Function.
function resend(_sock, downstream) {

  // check if any
  if ( _sock.commits[downstream] !== undefined ) {
 
    _log('re-send these messages', downstream);
    
  	for ( var idb in _sock.commits[downstream] ) {

  	  var msg = _sock.commits[downstream][idb];
      var messages = msg.messages;

      delete msg.messages;
      delete msg.downstream;
      msg.messages = [];
        
      // reform message.
		  for ( var i in messages ) {
		    msg.messages.push(messages[i]);
		  }
		  
		  //_log('msg> ' + util.inspect(msg, true, 99, true));
       
      delete _sock.commits[downstream][idb];
      
      //_log('resend msg');
      _sock.send(msg);  // resend message.
      
    }
    
    if ( Object.keys(_sock.commits[downstream]).length === 0 ) {
        //_log('nothing left in downstream _sock.commits so delete object from _sock.commits.');
        delete _sock.commits[downstream];    	
    }   
    
  }

}




