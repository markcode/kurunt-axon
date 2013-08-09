
/**
 * Module dependencies.
 */

var debug = require('debug')('axon:guarantee');

/**
 * Guarantee plugin.
 *
 * Provides an `.enqueue()` method for `guarantee` module.
 * Messages are passed to `enqueue` will be buffered until sent sock (rep)
 * replys with commit (have processed it), else if sock `disconnects` will
 * resend to another sock, hence guaranteeing delivery.
 *
 * Emits:
 *
 *  - `drop` (msg) when a message is dropped
 *  - `flush` (msgs) when the queue is flushed
 *
 * @param {Object} options
 * @api private
 */
 
 
var util 					= require('util');




exports.enqueue = enqueue;
var queue = {};


function enqueue(sock, m) {

  var msg = m[0];  // get message out of args.
	queue[msg.idb] = msg;
	//queue[msg.idb]['messages'] = {};
	//delete msg['message];
	queue[msg.idb]['msgs'] = {};

  console.log('peername> ' + util.inspect(sock._peername, true, 99, true));
  console.log('msg> ' + util.inspect(msg, true, 99, true));
	
  console.log('msg.messages> ' + util.inspect(msg.messages, true, 99, true));
	
  var x = 0;
  for ( x = 0; x < msg.messages.length; x++ ) {
  	console.log('mc: ' + msg.messages[x]);
  	queue[msg.idb]['msgs'][x + 1] = msg.messages[x];
  }
	
/*
	// que this message block, until downstream node has confirmed comitment.
	var x = 0;
	queue[msg.idb] = msg;
	queue[msg.idb]['messages'] = {};
	for ( x = 0; x < msg.messages.length; x++ ) {
		console.log('m: ' + msg.messages[x]);
		queue[msg.idb].messages[x + 1] = msg.messages[x];
	}
	//console.log('que> ' + util.inspect(que, true, 99, true));
	//delete que[idb].msgs['2'];
	//console.log('que2> ' + util.inspect(que, true, 99, true));	
	//delete que[idb];
	//console.log('que3> ' + util.inspect(que, true, 99, true));	

*/

 // queue.push(msg);
  console.log('queue> ' + util.inspect(queue, true, 99, true));
      
      
}

