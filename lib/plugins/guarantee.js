
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
 *  - `drop` (msg) when a message is dropped
 *  - `flush` (msgs) when the queue is flushed
 *
 * @param {Object} options
 * @api private
 */
 
 
var util = require('util');
var RESEND_DELAY = 1000;



module.exports = function(options){
  options = options || {};

  return function(sock){


    sock.commits = {};  // object array containing each message waiting for commitment.



    // if disconnect event, check commits for any unprocessed messages and resend.
    sock.on('disconnect', function(_s){
      var downstream = _s._peername.address + ':' + _s._peername.port.toString();
      console.log('downstream disconnect: ' + downstream);
      var _sock = this;
      if ( _sock.commits[downstream] !== undefined ) {
        setTimeout(function(){
          resend(_sock, downstream);
        }, RESEND_DELAY);
      }
    });



		// commit Function.
    sock.commit = function(s, msg){

      var _sock = this;
     
      var downstream = s._peername.address + ':' + s._peername.port.toString();

      console.log('comit this message');
      var m = msg[0];  // get message out of args.
      var messages = msg[0].messages;
      
      var commits_hwm = _sock.settings.commits_hwm;
      //console.log('commits_hwm: ' + commits_hwm);
      var commits_size = Buffer.byteLength(JSON.stringify(sock.commits), 'utf8');
      //console.log('commits_size: ' + commits_size);
      if ( commits_size >= commits_hwm ) {
        //console.log('commits_hwm exceeded');
        return commit_drop(m);      
      }
      
      console.log('msg> ' + util.inspect(m, true, 99, true));

      // add messages to queue.
      if ( _sock.commits[downstream] === undefined ) {
        _sock.commits[downstream] = {};
      }
	
      _sock.commits[downstream][m.idb] = m;
      delete _sock.commits[downstream][m.idb].messages;
      _sock.commits[downstream][m.idb]['messages'] = {};

      //console.log('peername> ' + util.inspect(sock._peername, true, 99, true));
      //console.log('msg> ' + util.inspect(msg, true, 99, true));
	
      // commits this message block, until downstream node has confirmed comitment.
      var x = 0;
      for ( x = 0; x < messages.length; x++ ) {
  	    //console.log('mc: ' + messages[x]);
  	    _sock.commits[downstream][m.idb]['messages'][(x + 1).toString()] = messages[x];
      }

      console.log('commits> ' + util.inspect(_sock.commits, true, 99, true));

      return downstream;
      
    };
    
    
    /**
     * Drop the given `msg`.
     */

    function commit_drop(msg) {
      debug('commit_drop');
      console.log('commit_drop-ed');
      sock.emit('commit_drop', msg);
    }      
    
  
    // committed Function.
    sock.committed = function(msg){

      var _sock = this;
     
      var m = msg[0];  // get message out of args.
      console.log('committed this msg');
      console.log('msg> ' + util.inspect(m, true, 99, true));
      

      var downstream = m.downstream;
      console.log('downstream: ' + downstream);
      
      if ( downstream !== undefined ) {
      	
        console.log('_sock.commits2> ' + util.inspect(_sock.commits[downstream], true, 99, true));
        // (msg.downstream, msg.idb, msg.messages)

        if ( _sock.commits[m.downstream] !== undefined ) {
          console.log('YES remove from q');
    
          //  console.log('msg.messages> ' + util.inspect(msg.messages, true, 99, true));
          var x = 0;
          // go through each message id returned by downstream and delete from queue.
          for ( x = 0; x < m.messages.length; x++ ) {
    
            var mx = m.messages[x];
            console.log('message to remove from _sock.commits: ' +mx);
     
            // console.log('q> ' + util.inspect(queue[msg.downstream][msg.idb]['messages'], true, 99, true));
            if ( _sock.commits[m.downstream][m.idb]['messages'][mx] !== undefined ) {
              console.log('Yes m in q delete');
              delete _sock.commits[m.downstream][m.idb]['messages'][mx];
            }
      
          }

          if ( Object.keys(_sock.commits[m.downstream][m.idb]['messages']).length === 0 ) {
            console.log('no message left in _sock.commits, so can delete idb from _sock.commits.');
            delete _sock.commits[m.downstream][m.idb];    	
          }
    
          if ( Object.keys(_sock.commits[m.downstream]).length === 0 ) {
            console.log('nothing left in downstream _sock.commits so delete object from _sock.commits.');
            delete _sock.commits[m.downstream];    	
          }    

        }

      }

      console.log('_sock.commits3> ' + util.inspect(_sock.commits, true, 99, true));      
      
    };   
    
   
  };
};



// resend Function.
function resend(_sock, downstream) {

  // check if any
  if ( _sock.commits[downstream] !== undefined ) {
 
    console.log('re-send these messages: ' + downstream);
    
  	for ( var idb in _sock.commits[downstream] ) {

    	// console.log('idb> ' + util.inspect(idb, true, 99, true));
  	  var msg = _sock.commits[downstream][idb];
      var messages = msg.messages;
    	//console.log('msgz> ' + util.inspect(msg, true, 99, true));
      delete msg.messages;
      delete msg.downstream;
      msg.messages = [];
        
      // reform message.
		  for ( var i in messages ) {
		    //console.log('i: ' + messages[i]);
		    msg.messages.push(messages[i]);
		  }
		  
		  console.log('msg> ' + util.inspect(msg, true, 99, true));
       
      delete _sock.commits[downstream][idb];
      
      console.log('resend msg');
      _sock.send(msg);  // resend message.
      
    }
    
    if ( Object.keys(_sock.commits[downstream]).length === 0 ) {
        console.log('nothing left in downstream _sock.commits so delete object from _sock.commits.');
        delete _sock.commits[downstream];    	
    }   
    
  }

}

















/*
// globals.
var queue = {};
var socks = undefined;
var sock = undefined;
var ReqSocket = undefined;
var enqueue = undefined;


//exports.enqueue = function(rs, s, ss, en, m) {
exports.enqueue = function(rs, s, ss, en, m) {


  socks = ss;
  sock = s;
  ReqSocket = rs;
  enqueue = en;
  //console.log('enqueue> ' + util.inspect(enqueue, true, 99, true));


  // may need have this as an array and push each into it.
  // if a downstream socket disconnects, check if any messages in the queue and resend (to another socket).
  //sock.on('close', disconnected);


  // add messages to queue.
  var downstream = sock._peername.address + ':' + sock._peername.port.toString();
  if ( queue[downstream] === undefined ) {
    queue[downstream] = {};
  }
	
	
  var msg = m[0];  // get message out of args.
  var messages = m[0].messages;
  
  queue[downstream][msg.idb] = msg;
  delete queue[downstream][msg.idb].messages;
  queue[downstream][msg.idb]['messages'] = {};

  //console.log('peername> ' + util.inspect(sock._peername, true, 99, true));
  //console.log('msg> ' + util.inspect(msg, true, 99, true));
	
  // que this message block, until downstream node has confirmed comitment.
  var x = 0;
  for ( x = 0; x < messages.length; x++ ) {
  	//console.log('mc: ' + messages[x]);
  	queue[downstream][msg.idb]['messages'][(x + 1).toString()] = messages[x];
  }


  console.log('queue> ' + util.inspect(queue, true, 99, true));
      
      
  return downstream;
      
};



exports.dequeue = function(m) {

  var msg = m[0];  // get message out of args. 

  //console.log('dequeue.m> ' + util.inspect(msg, true, 99, true));

  var downstream = msg.downstream;
  console.log('downstream: ' + downstream);
  
  
 console.log('dequeue2> ' + util.inspect(queue[downstream], true, 99, true));
  // (msg.downstream, msg.idb, msg.messages)


 // var q = queue[msg.downstream];

  if ( queue[msg.downstream] !== undefined ) {
    console.log('YES remove from q');
    
    

    
  //  console.log('msg.messages> ' + util.inspect(msg.messages, true, 99, true));
    var x = 0;
    // go through each message id returned by downstream and delete from queue.
    for ( x = 0; x < msg.messages.length; x++ ) {
    
      var mx = msg.messages[x];
      console.log('message to remove from queue: ' +mx);
      
      
     // console.log('q> ' + util.inspect(queue[msg.downstream][msg.idb]['messages'], true, 99, true));
      if ( queue[msg.downstream][msg.idb]['messages'][mx] !== undefined ) {
        console.log('Yes m in q delete');
        delete queue[msg.downstream][msg.idb]['messages'][mx];
      }
      
    }

    
    
    
    if ( Object.keys(queue[msg.downstream][msg.idb]['messages']).length === 0 ) {
        console.log('no message left in queue, so can delete idb from queue.');
        delete queue[msg.downstream][msg.idb];    	
    }
    
    if ( Object.keys(queue[msg.downstream]).length === 0 ) {
        console.log('nothing left in downstream queue so delete object from queue.');
        delete queue[msg.downstream];    	
    }    
    
    
  }



  console.log('dequeue3> ' + util.inspect(queue, true, 99, true));
};
*/





