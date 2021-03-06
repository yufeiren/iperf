/*--------------------------------------------------------------- 
 * Copyright (c) 1999,2000,2001,2002,2003                              
 * The Board of Trustees of the University of Illinois            
 * All Rights Reserved.                                           
 *--------------------------------------------------------------- 
 * Permission is hereby granted, free of charge, to any person    
 * obtaining a copy of this software (Iperf) and associated       
 * documentation files (the "Software"), to deal in the Software  
 * without restriction, including without limitation the          
 * rights to use, copy, modify, merge, publish, distribute,        
 * sublicense, and/or sell copies of the Software, and to permit     
 * persons to whom the Software is furnished to do
 * so, subject to the following conditions: 
 *
 *     
 * Redistributions of source code must retain the above 
 * copyright notice, this list of conditions and 
 * the following disclaimers. 
 *
 *     
 * Redistributions in binary form must reproduce the above 
 * copyright notice, this list of conditions and the following 
 * disclaimers in the documentation and/or other materials 
 * provided with the distribution. 
 * 
 *     
 * Neither the names of the University of Illinois, NCSA, 
 * nor the names of its contributors may be used to endorse 
 * or promote products derived from this Software without
 * specific prior written permission. 
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES 
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND 
 * NONINFRINGEMENT. IN NO EVENT SHALL THE CONTIBUTORS OR COPYRIGHT 
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, 
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, 
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE. 
 * ________________________________________________________________
 * National Laboratory for Applied Network Research 
 * National Center for Supercomputing Applications 
 * University of Illinois at Urbana-Champaign 
 * http://www.ncsa.uiuc.edu
 * ________________________________________________________________ 
 *
 * Client.cpp
 * by Mark Gates <mgates@nlanr.net>
 * -------------------------------------------------------------------
 * A client thread initiates a connect to the server and handles
 * sending and receiving data, then closes the socket.
 * ------------------------------------------------------------------- */

#include "headers.h"
#include "Client.hpp"
#include "Thread.h"
#include "SocketAddr.h"
#include "PerfSocket.hpp"
#include "Extractor.h"
#include "delay.hpp"
#include "util.h"
#include "Locale.h"
#include "rdma.h"

/* -------------------------------------------------------------------
 * Store server hostname, optionally local hostname, and socket info.
 * ------------------------------------------------------------------- */

Client::Client( thread_Settings *inSettings ) {
    mSettings = inSettings;
    mBuf = NULL;

    // initialize buffer
    mBuf = new char[ mSettings->mBufLen ];
    pattern( mBuf, mSettings->mBufLen );
    if ( isFileInput( mSettings ) ) {
        if ( !isSTDIN( mSettings ) )
            Extractor_Initialize( mSettings->mFileName, mSettings->mBufLen, mSettings );
        else
            Extractor_InitializeFile( stdin, mSettings->mBufLen, mSettings );

        if ( !Extractor_canRead( mSettings ) ) {
            unsetFileInput( mSettings );
        }
    }

    // connect TCP/UDP
    if ( mSettings->mThreadMode == kMode_Client )
    	Connect( );
    else if ( mSettings->mThreadMode == kMode_RDMA_Client ) {// connect RDMA
    	mCb = new rdma_cb;
	Settings_Initialize_Cb( mCb );
	mCb->size = mSettings->mBufLen;
	DPRINTF(("client buffer size is %d\n", mCb->size));
	rdma_init( mCb );

	{
	// addr
/*	if ( mSettings->mThreadMode == kMode_RDMA_Listener)
		memcpy( &mCb->sin, &mSettings->local, sizeof(iperf_sockaddr));
	else if ( mSettings->mThreadMode == kMode_RDMA_Client)
		memcpy( &mCb->sin, &mSettings->peer, sizeof(iperf_sockaddr));
*/
	// port
	mCb->port = mSettings->mPort;
	DPRINTF(("connecting port is %d\n", mCb->port));
	
	mCb->server = 0;
	
	mCb->size = mSettings->mBufLen;
	DPRINTF(("client buffer size is %d\n", mCb->size));
	
	switch ( mSettings->mMode ) {
	case kTest_RDMA_ActRead:
	    mCb->trans_mode = kRdmaTrans_ActRead;
	    break;
	case kTest_RDMA_ActWrte:
	    mCb->trans_mode = kRdmaTrans_ActWrte;
	    break;
	case kTest_RDMA_PasRead:
	    mCb->trans_mode = kRdmaTrans_PasRead;
	    break;
	case kTest_RDMA_PasWrte:
	    mCb->trans_mode = kRdmaTrans_PasWrte;
	    break;
	default:
	    fprintf(stderr, "unrecognize transfer mode %d\n", mSettings->mMode);
	    break;
	} // end switch
	
	
	}
	ConnectRDMA( );
    }
    else
    	fprintf(stderr, "err thread mode: %d\n", mSettings->mThreadMode);

    if ( isReport( inSettings ) ) {
        ReportSettings( inSettings );
        if ( mSettings->multihdr && isMultipleReport( inSettings ) ) {
            mSettings->multihdr->report->connection.peer = mSettings->peer;
            mSettings->multihdr->report->connection.size_peer = mSettings->size_peer;
            mSettings->multihdr->report->connection.local = mSettings->local;
            SockAddr_setPortAny( &mSettings->multihdr->report->connection.local );
            mSettings->multihdr->report->connection.size_local = mSettings->size_local;
        }
    }

} // end Client

/* -------------------------------------------------------------------
 * Delete memory (hostname strings).
 * ------------------------------------------------------------------- */

Client::~Client() {
    if ( mSettings->mSock != INVALID_SOCKET ) {
        int rc = close( mSettings->mSock );
        WARN_errno( rc == SOCKET_ERROR, "close" );
        mSettings->mSock = INVALID_SOCKET;
    }
    DELETE_ARRAY( mBuf );
} // end ~Client

const double kSecs_to_usecs = 1e6; 
const int    kBytes_to_Bits = 8; 

void Client::RunTCP( void ) {
    unsigned long currLen = 0; 
    struct itimerval it;
    max_size_t totLen = 0;

    int err;

    char* readAt = mBuf;

    // Indicates if the stream is readable 
    bool canRead = true, mMode_Time = isModeTime( mSettings ); 

    ReportStruct *reportstruct = NULL;

    // InitReport handles Barrier for multiple Streams
    mSettings->reporthdr = InitReport( mSettings );
    reportstruct = new ReportStruct;
    reportstruct->packetID = 0;

    lastPacketTime.setnow();
    if ( mMode_Time ) {
	memset (&it, 0, sizeof (it));
	it.it_value.tv_sec = (int) (mSettings->mAmount / 100.0);
	it.it_value.tv_usec = (int) 10000 * (mSettings->mAmount -
	    it.it_value.tv_sec * 100.0);
	err = setitimer( ITIMER_REAL, &it, NULL );
	if ( err != 0 ) {
	    perror("setitimer");
	    exit(1);
	}
    }
    do {
        // Read the next data block from 
        // the file if it's file input 
        if ( isFileInput( mSettings ) ) {
            Extractor_getNextDataBlock( readAt, mSettings ); 
            canRead = Extractor_canRead( mSettings ) != 0; 
        } else
            canRead = true; 

        // perform write 
        currLen = write( mSettings->mSock, mBuf, mSettings->mBufLen ); 
        if ( currLen < 0 ) {
            WARN_errno( currLen < 0, "write2" ); 
            break; 
        }
	totLen += currLen;

	if(mSettings->mInterval > 0) {
    	    gettimeofday( &(reportstruct->packetTime), NULL );
            reportstruct->packetLen = currLen;
            ReportPacket( mSettings->reporthdr, reportstruct );
        }	

        if ( !mMode_Time ) {
            /* mAmount may be unsigned, so don't let it underflow! */
            if( mSettings->mAmount >= currLen ) {
                mSettings->mAmount -= currLen;
            } else {
                mSettings->mAmount = 0;
            }
        }

    } while ( ! (sInterupted  || 
                   (!mMode_Time  &&  0 >= mSettings->mAmount)) && canRead ); 

    // stop timing
    gettimeofday( &(reportstruct->packetTime), NULL );

    // if we're not doing interval reporting, report the entire transfer as one big packet
    if(0.0 == mSettings->mInterval) {
        reportstruct->packetLen = totLen;
        ReportPacket( mSettings->reporthdr, reportstruct );
    }
    CloseReport( mSettings->reporthdr, reportstruct );

    DELETE_PTR( reportstruct );
    EndReport( mSettings->reporthdr );
}


void Client::RunRDMA( void ) {
    unsigned long currLen = 0; 
    struct itimerval it;
    max_size_t totLen = 0;

    int err;

    char* readAt = mBuf;

    struct ibv_send_wr* bad_wr;
    
    // Indicates if the stream is readable 
    bool canRead = true, mMode_Time = isModeTime( mSettings ); 

    ReportStruct *reportstruct = NULL;

    // InitReport handles Barrier for multiple Streams
    mSettings->reporthdr = InitReport( mSettings );
    reportstruct = new ReportStruct;
    reportstruct->packetID = 0;

    lastPacketTime.setnow();
    if ( mMode_Time ) {
	memset (&it, 0, sizeof (it));
	it.it_value.tv_sec = (int) (mSettings->mAmount / 100.0);
	it.it_value.tv_usec = (int) 10000 * (mSettings->mAmount -
	    it.it_value.tv_sec * 100.0);
	err = setitimer( ITIMER_REAL, &it, NULL );
	if ( err != 0 ) {
	    perror("setitimer");
	    exit(1);
	}
    }

    do {
        // Read the next data block from 
        // the file if it's file input 
        if ( isFileInput( mSettings ) 
		&& ( (mCb->trans_mode == kRdmaTrans_ActWrte) ||
		(mCb->trans_mode == kRdmaTrans_PasRead) ) ) {
            // Extractor_getNextDataBlock( readAt, mSettings );
            mCb->size = Extractor_getNextDataBlock( mCb->start_buf, mSettings );
            canRead = Extractor_canRead( mSettings ) != 0;
        } else
            canRead = true;

        // perform RDMA read or write
//        currLen = write( mSettings->mSock, mBuf, mSettings->mBufLen );
	switch ( mCb->trans_mode ) {
	case kRdmaTrans_ActRead:
		break;
	case kRdmaTrans_ActWrte:
		break;
	case kRdmaTrans_PasRead:
		currLen = cli_pas_rdma_rd( mCb );
		break;
	case kRdmaTrans_PasWrte:
		currLen = cli_pas_rdma_wr( mCb );
		break;
	default:
		fprintf(stderr, "unrecognized transfer mode %d\n", \
			mCb->trans_mode);
		break;
	}
	
/*	DPRINTF(("client start transfer data via rdma\n"));
	mCb->state = RDMA_READ_ADV;
	
	iperf_format_send(mCb, mCb->start_buf, mCb->start_mr);

	err = ibv_post_send(mCb->qp, &mCb->sq_wr, &bad_wr);
	if (err) {
		fprintf(stderr, "post send error %d\n", err);
		break;
	}
	DPRINTF(("client ibv_post_send success\n"));

	/* Wait for server to ACK read complete */
/*	DPRINTF(("client RunRDMA cb @ %x\n", (unsigned long)mCb));
	DPRINTF(("client RunRDMA sem_wait @ %x\n", (unsigned long)&mCb->sem));
	DPRINTF(("wait server to say go ahead\n"));
	sem_wait(&mCb->sem);
	if (mCb->state != RDMA_WRITE_ADV) {
		fprintf(stderr, "wait for RDMA_WRITE_ADV state %d\n",
			mCb->state);
		err = -1;
		break;
	}
	DPRINTF(("client Wait for server to ACK read complete success\n"));
	

	iperf_format_send(mCb, mCb->rdma_buf, mCb->rdma_mr);
	err = ibv_post_send(mCb->qp, &mCb->sq_wr, &bad_wr);
	if (err) {
		fprintf(stderr, "post send error %d\n", err);
		break;
	}

	/* Wait for the server to say the RDMA Write is complete.
	sem_wait(&mCb->sem);
	if (mCb->state != RDMA_WRITE_COMPLETE) {
		fprintf(stderr, "wait for RDMA_WRITE_COMPLETE state %d\n",
			mCb->state);
		err = -1;
		break;
	} */
	
//	currLen = 2 * ( mCb->size + sizeof( iperf_rdma_info ) );
//	currLen = 2 * mCb->size;
	// iperf_rdma_info is transfer via rdma recv/send
	
        if ( currLen < 0 ) {
            WARN_errno( currLen < 0, "write2" ); 
            break;
        }
	totLen += currLen;
	
	if( mSettings->mInterval > 0 ) {
    	    gettimeofday( &(reportstruct->packetTime), NULL );
            reportstruct->packetLen = currLen;
            ReportPacket( mSettings->reporthdr, reportstruct );
        }

        if ( !mMode_Time ) {
            /* mAmount may be unsigned, so don't let it underflow! */
            if( mSettings->mAmount >= currLen ) {
                mSettings->mAmount -= currLen;
            } else {
                mSettings->mAmount = 0;
            }
        }

    } while ( ! (sInterupted  || 
                   (!mMode_Time  &&  0 >= mSettings->mAmount)) && canRead ); 

    // stop timing
    gettimeofday( &(reportstruct->packetTime), NULL );

    // if we're not doing interval reporting, report the entire transfer as one big packet
    if(0.0 == mSettings->mInterval) {
        reportstruct->packetLen = totLen;
        ReportPacket( mSettings->reporthdr, reportstruct );
    }
    CloseReport( mSettings->reporthdr, reportstruct );

    DELETE_PTR( reportstruct );
    EndReport( mSettings->reporthdr );
}


/* ------------------------------------------------------------------- 
 * Send data using the connected UDP/TCP socket, 
 * until a termination flag is reached. 
 * Does not close the socket. 
 * ------------------------------------------------------------------- */ 

void Client::Run( void ) {
    struct UDP_datagram* mBuf_UDP = (struct UDP_datagram*) mBuf; 
    unsigned long currLen = 0; 

    int delay_target = 0; 
    int delay = 0; 
    int adjust = 0; 

    char* readAt = mBuf;

#if HAVE_THREAD
    if ( !isUDP( mSettings ) ) {
	RunTCP();
	return;
    }
#endif
    
    // Indicates if the stream is readable 
    bool canRead = true, mMode_Time = isModeTime( mSettings ); 

    // setup termination variables
    if ( mMode_Time ) {
        mEndTime.setnow();
        mEndTime.add( mSettings->mAmount / 100.0 );
    }

    if ( isUDP( mSettings ) ) {
        // Due to the UDP timestamps etc, included 
        // reduce the read size by an amount 
        // equal to the header size
    
        // compute delay for bandwidth restriction, constrained to [0,1] seconds 
        delay_target = (int) ( mSettings->mBufLen * ((kSecs_to_usecs * kBytes_to_Bits) 
                                                     / mSettings->mUDPRate) ); 
        if ( delay_target < 0  || 
             delay_target > (int) 1 * kSecs_to_usecs ) {
            fprintf( stderr, warn_delay_large, delay_target / kSecs_to_usecs ); 
            delay_target = (int) kSecs_to_usecs * 1; 
        }
        if ( isFileInput( mSettings ) ) {
            if ( isCompat( mSettings ) ) {
                Extractor_reduceReadSize( sizeof(struct UDP_datagram), mSettings );
                readAt += sizeof(struct UDP_datagram);
            } else {
                Extractor_reduceReadSize( sizeof(struct UDP_datagram) +
                                          sizeof(struct client_hdr), mSettings );
                readAt += sizeof(struct UDP_datagram) +
                          sizeof(struct client_hdr);
            }
        }
    }

    ReportStruct *reportstruct = NULL;

    // InitReport handles Barrier for multiple Streams
    mSettings->reporthdr = InitReport( mSettings );
    reportstruct = new ReportStruct;
    reportstruct->packetID = 0;

    lastPacketTime.setnow();
    
    do {

        // Test case: drop 17 packets and send 2 out-of-order: 
        // sequence 51, 52, 70, 53, 54, 71, 72 
        //switch( datagramID ) { 
        //  case 53: datagramID = 70; break; 
        //  case 71: datagramID = 53; break; 
        //  case 55: datagramID = 71; break; 
        //  default: break; 
        //} 
        gettimeofday( &(reportstruct->packetTime), NULL );

        if ( isUDP( mSettings ) ) {
            // store datagram ID into buffer 
            mBuf_UDP->id      = htonl( (reportstruct->packetID)++ ); 
            mBuf_UDP->tv_sec  = htonl( reportstruct->packetTime.tv_sec ); 
            mBuf_UDP->tv_usec = htonl( reportstruct->packetTime.tv_usec );

            // delay between writes 
            // make an adjustment for how long the last loop iteration took 
            // TODO this doesn't work well in certain cases, like 2 parallel streams 
            adjust = delay_target + lastPacketTime.subUsec( reportstruct->packetTime ); 
            lastPacketTime.set( reportstruct->packetTime.tv_sec, 
                                reportstruct->packetTime.tv_usec ); 

            if ( adjust > 0  ||  delay > 0 ) {
                delay += adjust; 
            }
        }

        // Read the next data block from 
        // the file if it's file input 
        if ( isFileInput( mSettings ) ) {
            Extractor_getNextDataBlock( readAt, mSettings ); 
            canRead = Extractor_canRead( mSettings ) != 0; 
        } else
            canRead = true; 

        // perform write 
        currLen = write( mSettings->mSock, mBuf, mSettings->mBufLen ); 
        if ( currLen < 0 && errno != ENOBUFS ) {
            WARN_errno( currLen < 0, "write2" ); 
            break; 
        }

        // report packets 
        reportstruct->packetLen = currLen;
        ReportPacket( mSettings->reporthdr, reportstruct );
        
        if ( delay > 0 ) {
            delay_loop( delay ); 
        }
        if ( !mMode_Time ) {
            /* mAmount may be unsigned, so don't let it underflow! */
            if( mSettings->mAmount >= currLen ) {
                mSettings->mAmount -= currLen;
            } else {
                mSettings->mAmount = 0;
            }
        }

    } while ( ! (sInterupted  || 
                 (mMode_Time   &&  mEndTime.before( reportstruct->packetTime ))  || 
                 (!mMode_Time  &&  0 >= mSettings->mAmount)) && canRead ); 

    // stop timing
    gettimeofday( &(reportstruct->packetTime), NULL );
    CloseReport( mSettings->reporthdr, reportstruct );

    if ( isUDP( mSettings ) ) {
        // send a final terminating datagram 
        // Don't count in the mTotalLen. The server counts this one, 
        // but didn't count our first datagram, so we're even now. 
        // The negative datagram ID signifies termination to the server. 
    
        // store datagram ID into buffer 
        mBuf_UDP->id      = htonl( -(reportstruct->packetID)  ); 
        mBuf_UDP->tv_sec  = htonl( reportstruct->packetTime.tv_sec ); 
        mBuf_UDP->tv_usec = htonl( reportstruct->packetTime.tv_usec ); 

        if ( isMulticast( mSettings ) ) {
            write( mSettings->mSock, mBuf, mSettings->mBufLen ); 
        } else {
            write_UDP_FIN( ); 
        }
    }
    DELETE_PTR( reportstruct );
    EndReport( mSettings->reporthdr );
} 
// end Run

void Client::InitiateServer() {
    if ( !isCompat( mSettings ) ) {
        int currLen;
        client_hdr* temp_hdr;
        if ( isUDP( mSettings ) ) {
            UDP_datagram *UDPhdr = (UDP_datagram *)mBuf;
            temp_hdr = (client_hdr*)(UDPhdr + 1);
        } else {
            temp_hdr = (client_hdr*)mBuf;
        }
        Settings_GenerateClientHdr( mSettings, temp_hdr );
        if ( !isUDP( mSettings ) ) {
            currLen = send( mSettings->mSock, mBuf, sizeof(client_hdr), 0 );
            if ( currLen < 0 ) {
                WARN_errno( currLen < 0, "write1" );
            }
        }
    }
}

/* -------------------------------------------------------------------
 * Setup a socket connected to a server.
 * If inLocalhost is not null, bind to that address, specifying
 * which outgoing interface to use.
 * ------------------------------------------------------------------- */

void Client::Connect( ) {
    int rc;
    SockAddr_remoteAddr( mSettings );

    assert( mSettings->inHostname != NULL );

    // create an internet socket
    int type = ( isUDP( mSettings )  ?  SOCK_DGRAM : SOCK_STREAM);

    int domain = (SockAddr_isIPv6( &mSettings->peer ) ? 
#ifdef HAVE_IPV6
                  AF_INET6
#else
                  AF_INET
#endif
                  : AF_INET);

    mSettings->mSock = socket( domain, type, 0 );
    WARN_errno( mSettings->mSock == INVALID_SOCKET, "socket" );

    SetSocketOptions( mSettings );


    SockAddr_localAddr( mSettings );
    if ( mSettings->mLocalhost != NULL ) {
        // bind socket to local address
        rc = bind( mSettings->mSock, (sockaddr*) &mSettings->local, 
                   SockAddr_get_sizeof_sockaddr( &mSettings->local ) );
        WARN_errno( rc == SOCKET_ERROR, "bind" );
    }

    // connect socket
    rc = connect( mSettings->mSock, (sockaddr*) &mSettings->peer, 
                  SockAddr_get_sizeof_sockaddr( &mSettings->peer ));
    FAIL_errno( rc == SOCKET_ERROR, "connect", mSettings );

    getsockname( mSettings->mSock, (sockaddr*) &mSettings->local, 
                 &mSettings->size_local );
    getpeername( mSettings->mSock, (sockaddr*) &mSettings->peer,
                 &mSettings->size_peer );
} // end Connect


/* -------------------------------------------------------------------
 * Setup a socket connected to a server use librdmacm.
 * If inLocalhost is not null, bind to that address, specifying
 * which outgoing interface to use.
 * ------------------------------------------------------------------- */

void Client::ConnectRDMA( ) {
    int rc;
    struct ibv_recv_wr* bad_wr;
    SockAddr_remoteAddr( mSettings );

    assert( mSettings->inHostname != NULL );

    // create an internet socket
    int type = ( isUDP( mSettings )  ?  SOCK_DGRAM : SOCK_STREAM);

    int domain = (SockAddr_isIPv6( &mSettings->peer ) ? 
#ifdef HAVE_IPV6
                  AF_INET6
#else
                  AF_INET
#endif
                  : AF_INET);
/*
    mSettings->mSock = socket( domain, type, 0 );
    WARN_errno( mSettings->mSock == INVALID_SOCKET, "socket" );

    SetSocketOptions( mSettings );


    SockAddr_localAddr( mSettings );
    if ( mSettings->mLocalhost != NULL ) {
        // bind socket to local address
        rc = bind( mSettings->mSock, (sockaddr*) &mSettings->local, 
                   SockAddr_get_sizeof_sockaddr( &mSettings->local ) );
        WARN_errno( rc == SOCKET_ERROR, "bind" );
    }

    // connect socket
    rc = connect( mSettings->mSock, (sockaddr*) &mSettings->peer, 
                  SockAddr_get_sizeof_sockaddr( &mSettings->peer ));
    FAIL_errno( rc == SOCKET_ERROR, "connect", mSettings );
    
    getsockname( mSettings->mSock, (sockaddr*) &mSettings->local, 
                 &mSettings->size_local );
    getpeername( mSettings->mSock, (sockaddr*) &mSettings->peer,
                 &mSettings->size_peer );
*/

	if (domain == AF_INET)
		((struct sockaddr_in *) &mCb->sin)->sin_port = htons(mCb->port);
	else
		((struct sockaddr_in6 *) &mCb->sin)->sin6_port = htons(mCb->port);

	rc = rdma_resolve_addr(mCb->cm_id, NULL, \
		(struct sockaddr *) &mSettings->peer, 2000);
	if (rc) {
		perror("rdma_resolve_addr");
		return;
	}

	sem_wait(&mCb->sem);
	if (mCb->state != ROUTE_RESOLVED) {
		fprintf(stderr, "waiting for addr/route resolution state %d\n",
			mCb->state);
		return;
	}

	rc = iperf_setup_qp(mCb, mCb->cm_id);
	if (rc) {
		fprintf(stderr, "iperf_setup_qp failed: %d\n", rc);
		return;
	}
	
	rc = iperf_setup_buffers(mCb);
	if (rc) {
		fprintf(stderr, "rdma_setup_buffers failed: %d\n", rc);
		goto err1;
	}
	
	rc = ibv_post_recv(mCb->qp, &mCb->rq_wr, &bad_wr);
	if (rc) {
		fprintf(stderr, "ibv_post_recv failed: %d\n", rc);
		goto err2;
	}

	pthread_create(&mCb->cqthread, NULL, cq_thread, mCb);

	rc = rdma_connect_client(mCb);
	if (rc) {
		fprintf(stderr, "connect error %d\n", rc);
		goto err2;
	}

	memcpy(&mSettings->local, rdma_get_local_addr(mCb->cm_id), \
		sizeof(iperf_sockaddr)) ;
	memcpy(&mSettings->peer, rdma_get_peer_addr(mCb->cm_id), \
		sizeof(iperf_sockaddr)) ;

	Mutex_Lock( &PseudoSockCond );
	mSettings->mSock = ++ PseudoSock;
	Mutex_Unlock( &PseudoSockCond );
	
	return;
//	rping_test_client(cb);
//	rdma_disconnect(cb->cm_id);
err2:
	iperf_free_buffers(mCb);
err1:
	iperf_free_qp(mCb);

} // end ConnectRDMA

/* ------------------------------------------------------------------- 
 * Send a datagram on the socket. The datagram's contents should signify 
 * a FIN to the application. Keep re-transmitting until an 
 * acknowledgement datagram is received. 
 * ------------------------------------------------------------------- */ 

void Client::write_UDP_FIN( ) {
    int rc; 
    fd_set readSet; 
    struct timeval timeout; 

    int count = 0; 
    while ( count < 10 ) {
        count++; 

        // write data 
        write( mSettings->mSock, mBuf, mSettings->mBufLen ); 

        // wait until the socket is readable, or our timeout expires 
        FD_ZERO( &readSet ); 
        FD_SET( mSettings->mSock, &readSet ); 
        timeout.tv_sec  = 0; 
        timeout.tv_usec = 250000; // quarter second, 250 ms 

        rc = select( mSettings->mSock+1, &readSet, NULL, NULL, &timeout ); 
        FAIL_errno( rc == SOCKET_ERROR, "select", mSettings ); 

        if ( rc == 0 ) {
            // select timed out 
            continue; 
        } else {
            // socket ready to read 
            rc = read( mSettings->mSock, mBuf, mSettings->mBufLen ); 
            WARN_errno( rc < 0, "read" );
    	    if ( rc < 0 ) {
                break;
            } else if ( rc >= (int) (sizeof(UDP_datagram) + sizeof(server_hdr)) ) {
                ReportServerUDP( mSettings, (server_hdr*) ((UDP_datagram*)mBuf + 1) );
            }

            return; 
        } 
    } 

    fprintf( stderr, warn_no_ack, mSettings->mSock, count ); 
} 
// end write_UDP_FIN 
