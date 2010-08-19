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
 * Server.cpp
 * by Mark Gates <mgates@nlanr.net>
 *     Ajay Tirumala (tirumala@ncsa.uiuc.edu>.
 * -------------------------------------------------------------------
 * A server thread is initiated for each connection accept() returns.
 * Handles sending and receiving data, and then closes socket.
 * Changes to this version : The server can be run as a daemon
 * ------------------------------------------------------------------- */

#define HEADERS()

#include "headers.h"
#include "Server.hpp"
#include "List.h"
#include "Extractor.h"
#include "Reporter.h"
#include "Locale.h"

/* -------------------------------------------------------------------
 * Stores connected socket and socket info.
 * ------------------------------------------------------------------- */

Server::Server( thread_Settings *inSettings ) {
    mSettings = inSettings;
    mBuf = NULL;

    // initialize buffer
    mBuf = new char[ mSettings->mBufLen ];
    FAIL_errno( mBuf == NULL, "No memory for buffer\n", mSettings );
    
    if ( inSettings->mThreadMode == kMode_RDMA_Server ){
	DPRINTF(("Server: kMode_RDMA_Server\n"));
	mCb = new rdma_cb;
	Settings_Initialize_Cb( mCb );
//	rdma_init( mCb );
	
	{
	// addr
/*	if ( mSettings->mThreadMode == kMode_RDMA_Listener)
		memcpy( &mCb->sin, &mSettings->local, sizeof(iperf_sockaddr));
	else if ( mSettings->mThreadMode == kMode_RDMA_Client)
		memcpy( &mCb->sin, &mSettings->peer, sizeof(iperf_sockaddr));
*/
	// port
	mCb->port = mSettings->mPort;
	
	mCb->server = 1;
	
	mCb->size = mSettings->mBufLen;
	DPRINTF(("server buffer size is %d\n", mCb->size));
	
	mCb->outputfile = mSettings->Output_file;
	}
	
	mCb->child_cm_id = inSettings->child_cm_id;
    	DPRINTF(("mCb->child_cm_id  %p\n", mCb->child_cm_id));
    }
	
}

/* -------------------------------------------------------------------
 * Destructor close socket.
 * ------------------------------------------------------------------- */

Server::~Server() {
    DPRINTF(("close %d\n", mSettings->mSock));
    if ( mSettings->mSock != INVALID_SOCKET ) {
        int rc = close( mSettings->mSock );
        WARN_errno( rc == SOCKET_ERROR, "close" );
        mSettings->mSock = INVALID_SOCKET;
    }
    DELETE_ARRAY( mBuf );
}

void Server::Sig_Int( int inSigno ) {
}

/* ------------------------------------------------------------------- 
 * Receive data from the (connected) socket.
 * Sends termination flag several times at the end. 
 * Does not close the socket. 
 * ------------------------------------------------------------------- */ 
void Server::Run( void ) {
    long currLen; 
    max_size_t totLen = 0;
    struct UDP_datagram* mBuf_UDP  = (struct UDP_datagram*) mBuf; 

    ReportStruct *reportstruct = NULL;

    reportstruct = new ReportStruct;
    if ( reportstruct != NULL ) {
        reportstruct->packetID = 0;
        mSettings->reporthdr = InitReport( mSettings );
        do {
            // perform read 
            currLen = recv( mSettings->mSock, mBuf, mSettings->mBufLen, 0 ); 
        
            if ( isUDP( mSettings ) ) {
                // read the datagram ID and sentTime out of the buffer 
                reportstruct->packetID = ntohl( mBuf_UDP->id ); 
                reportstruct->sentTime.tv_sec = ntohl( mBuf_UDP->tv_sec  );
                reportstruct->sentTime.tv_usec = ntohl( mBuf_UDP->tv_usec ); 
		reportstruct->packetLen = currLen;
		gettimeofday( &(reportstruct->packetTime), NULL );
            } else {
		totLen += currLen;
	    }
        
            // terminate when datagram begins with negative index 
            // the datagram ID should be correct, just negated 
            if ( reportstruct->packetID < 0 ) {
                reportstruct->packetID = -reportstruct->packetID;
                currLen = -1; 
            }

	    if ( isUDP (mSettings)) {
		ReportPacket( mSettings->reporthdr, reportstruct );
            } else if ( !isUDP (mSettings) && mSettings->mInterval > 0) {
                reportstruct->packetLen = currLen;
                gettimeofday( &(reportstruct->packetTime), NULL );
                ReportPacket( mSettings->reporthdr, reportstruct );
            }

            if (mSettings->Output_file != NULL)
	        if ( fwrite( mBuf, currLen, 1, mSettings->Output_file ) < 0 )
	            fprintf( stderr, "Unable to write to the file stream\n");

        } while ( currLen > 0 ); 
        
        
        // stop timing 
        gettimeofday( &(reportstruct->packetTime), NULL );
        
	if ( !isUDP (mSettings)) {
		if(0.0 == mSettings->mInterval) {
                        reportstruct->packetLen = totLen;
                }
		ReportPacket( mSettings->reporthdr, reportstruct );
	}
        CloseReport( mSettings->reporthdr, reportstruct );
        
        // send a acknowledgement back only if we're NOT receiving multicast 
        if ( isUDP( mSettings ) && !isMulticast( mSettings ) ) {
            // send back an acknowledgement of the terminating datagram 
            write_UDP_AckFIN( ); 
        }
    } else {
        FAIL(1, "Out of memory! Closing server thread\n", mSettings);
    }

    Mutex_Lock( &clients_mutex );     
    Iperf_delete( &(mSettings->peer), &clients ); 
    Mutex_Unlock( &clients_mutex );

    DELETE_PTR( reportstruct );
    EndReport( mSettings->reporthdr );
} 
// end Recv 

void Server::RunRDMA( void ) {
	DPRINTF(("in RunRDMA\n"));
//	rdma_cb *cb = NULL;
    
	struct ibv_send_wr *bad_send_wr;
	struct ibv_recv_wr *bad_recv_wr;
	int ret;
	
    long currLen; 
    max_size_t totLen = 0;
    struct UDP_datagram* mBuf_UDP  = (struct UDP_datagram*) mBuf; 

    ReportStruct *reportstruct = NULL;

    reportstruct = new ReportStruct;
    DPRINTF(("before Rdma_Settings_Copy\n"));
//    Rdma_Settings_Copy(mCb, &cb);
	DPRINTF(("before iperf_setup_qp\n"));
	ret = iperf_setup_qp(mCb, mCb->child_cm_id);
	if (ret) {
		fprintf(stderr, "setup_qp failed: %d\n", ret);
		goto err0;
	}
	DPRINTF(("iperf_setup_qp success\n"));

	DPRINTF(("before iperf_setup_buffers\n"));
	ret = iperf_setup_buffers(mCb);
	if (ret) {
		fprintf(stderr, "rping_setup_buffers failed: %d\n", ret);
		goto err1;
	}
	DPRINTF(("iperf_setup_buffers success\n"));

	DPRINTF(("before ibv_post_recv\n"));
	ret = ibv_post_recv(mCb->qp, &mCb->rq_wr, &bad_recv_wr);
	if (ret) {
		fprintf(stderr, "ibv_post_recv failed: %d\n", ret);
		goto err2;
	}
	DPRINTF(("ibv_post_recv success\n"));

	pthread_create(&mCb->cqthread, NULL, cq_thread, mCb);

	DPRINTF(("before iperf_accept\n"));
	ret = iperf_accept(mCb);
	if (ret) {
		fprintf(stderr, "accept error %d\n", ret);
		goto err3;
	}
	DPRINTF(("iperf_accept success\n"));
	
    if ( reportstruct != NULL ) {
        reportstruct->packetID = 0;
        mSettings->reporthdr = InitReport( mSettings );
        
        int first = 1;
        
        do {
            // perform read 
//            currLen = recv( mSettings->mSock, mBuf, mSettings->mBufLen, 0 ); 
            
	    DPRINTF(("server start transfer data via rdma\n"));
	    DPRINTF(("cb @ %x\n", (unsigned long)mCb));
	    DPRINTF(("server cb state: %d\n", mCb->state));
            	
            	
	        /*
	        mCb->state = RDMA_READ_ADV;
	        
		if (mCb->state != RDMA_READ_ADV) {
			fprintf(stderr, "wait for RDMA_READ_ADV state %d\n",
				mCb->state);
			ret = -1;
			break;
		}*/
		switch ( mCb->trans_mode ) {
		case kRdmaTrans_ActRead:
			if ( !first )
			    sem_wait(&mCb->sem);
			first = 0;
			currLen = svr_act_rdma_rd( mCb );
			break;
		case kRdmaTrans_ActWrte:
			currLen = svr_act_rdma_wr( mCb );
			break;
		case kRdmaTrans_PasRead:
			currLen = svr_pas_rdma_rd( mCb );
			break;
		case kRdmaTrans_PasWrte:
			currLen = svr_pas_rdma_wr( mCb );
			break;
		}
		DEBUG_LOG("server received sink adv\n");

		/* Issue RDMA Read.
		mCb->rdma_sq_wr.opcode = IBV_WR_RDMA_READ;
		mCb->rdma_sq_wr.wr.rdma.rkey = mCb->remote_rkey;
		mCb->rdma_sq_wr.wr.rdma.remote_addr = mCb->remote_addr;
		mCb->rdma_sq_wr.sg_list->length = mCb->remote_len;

		ret = ibv_post_send(mCb->qp, &mCb->rdma_sq_wr, &bad_send_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		}
		DEBUG_LOG("server posted rdma read req\n"); */

		/* Wait for read completion
		sem_wait(&mCb->sem);
		if (mCb->state != RDMA_READ_COMPLETE) {
			fprintf(stderr, "wait for RDMA_READ_COMPLETE state %d\n",
				mCb->state);
			ret = -1;
			break;
		}
		DEBUG_LOG("server received read complete\n"); */

		/* Display data in recv buf
		if (mCb->verbose)
			printf("server ping data: %s\n", mCb->rdma_buf); */
			
		/* Tell client to continue
		ret = ibv_post_send(mCb->qp, &mCb->sq_wr, &bad_send_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		}
		DEBUG_LOG("server posted go ahead\n"); */
// sleep(5);

		/* Wait for client's RDMA STAG/TO/Len
		sem_wait(&mCb->sem);
		if (mCb->state != RDMA_WRITE_ADV) {
			fprintf(stderr, "wait for RDMA_WRITE_ADV state %d\n",
				mCb->state);
			ret = -1;
			break;
		}
		DEBUG_LOG("server received sink adv\n"); */

		/* RDMA Write echo data
		mCb->rdma_sq_wr.opcode = IBV_WR_RDMA_WRITE;
		mCb->rdma_sq_wr.wr.rdma.rkey = mCb->remote_rkey;
		mCb->rdma_sq_wr.wr.rdma.remote_addr = mCb->remote_addr;
		mCb->rdma_sq_wr.sg_list->length = strlen(mCb->rdma_buf) + 1;
		DEBUG_LOG("rdma write from lkey %x laddr %x len %d\n",
			  mCb->rdma_sq_wr.sg_list->lkey,
			  mCb->rdma_sq_wr.sg_list->addr,
			  mCb->rdma_sq_wr.sg_list->length);

		ret = ibv_post_send(mCb->qp, &mCb->rdma_sq_wr, &bad_send_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		} */

		/* Wait for completion
		ret = sem_wait(&mCb->sem);
		if (mCb->state != RDMA_WRITE_COMPLETE) {
			fprintf(stderr, "wait for RDMA_WRITE_COMPLETE state %d\n",
				mCb->state);
			ret = -1;
			break;
		}
		DEBUG_LOG("server rdma write complete \n"); */

		/* Tell client to begin again
		ret = ibv_post_send(mCb->qp, &mCb->sq_wr, &bad_send_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		}
		DEBUG_LOG("server posted go ahead\n"); */
		
//            currLen = 2 * ( mCb->remote_len + sizeof( iperf_rdma_info ) );
            DEBUG_LOG("server: RDMA read %ld byte this time\n", currLen);
            
            if ( isUDP( mSettings ) ) {
                // read the datagram ID and sentTime out of the buffer 
                reportstruct->packetID = ntohl( mBuf_UDP->id ); 
                reportstruct->sentTime.tv_sec = ntohl( mBuf_UDP->tv_sec  );
                reportstruct->sentTime.tv_usec = ntohl( mBuf_UDP->tv_usec ); 
		reportstruct->packetLen = currLen;
		gettimeofday( &(reportstruct->packetTime), NULL );
            } else {
		totLen += currLen;
	    }
        
            // terminate when datagram begins with negative index 
            // the datagram ID should be correct, just negated 
            DPRINTF(("packetID %d\n", reportstruct->packetID));
	    if ( reportstruct->packetID < 0 ) {
                reportstruct->packetID = -reportstruct->packetID;
                currLen = -1; 
            }

	    if ( isUDP (mSettings)) {
		ReportPacket( mSettings->reporthdr, reportstruct );
            } else if ( !isUDP (mSettings) && mSettings->mInterval > 0) {
                reportstruct->packetLen = currLen;
                gettimeofday( &(reportstruct->packetTime), NULL );
                ReportPacket( mSettings->reporthdr, reportstruct );
            }
            
            DPRINTF(("server currLen = %d\n", currLen));

        } while ( currLen > 0 ); 
        
        
        // stop timing 
        gettimeofday( &(reportstruct->packetTime), NULL );
        
	if ( !isUDP (mSettings)) {
		if(0.0 == mSettings->mInterval) {
                        reportstruct->packetLen = totLen;
                }
		ReportPacket( mSettings->reporthdr, reportstruct );
	}
        CloseReport( mSettings->reporthdr, reportstruct );
        
        // send a acknowledgement back only if we're NOT receiving multicast 
        if ( isUDP( mSettings ) && !isMulticast( mSettings ) ) {
            // send back an acknowledgement of the terminating datagram 
            write_UDP_AckFIN( ); 
        }
    } else {
        FAIL(1, "Out of memory! Closing server thread\n", mSettings);
    }

    Mutex_Lock( &clients_mutex );     
    Iperf_delete( &(mSettings->peer), &clients ); 
    Mutex_Unlock( &clients_mutex );

//	iperf_test_server(cb);
	rdma_disconnect(mCb->child_cm_id);
	iperf_free_buffers(mCb);
	iperf_free_qp(mCb);
	pthread_cancel(mCb->cqthread);
	pthread_join(mCb->cqthread, NULL);
	rdma_destroy_id(mCb->child_cm_id);
	delete mCb;


    DELETE_PTR( reportstruct );
    EndReport( mSettings->reporthdr );
    
    return;
err3:
	pthread_cancel(mCb->cqthread);
	pthread_join(mCb->cqthread, NULL);
err2:
	iperf_free_buffers(mCb);
err1:
	iperf_free_qp(mCb);
err0:
	delete mCb;
	return;
} 

/* ------------------------------------------------------------------- 
 * Send an AckFIN (a datagram acknowledging a FIN) on the socket, 
 * then select on the socket for some time. If additional datagrams 
 * come in, probably our AckFIN was lost and they are re-transmitted 
 * termination datagrams, so re-transmit our AckFIN. 
 * ------------------------------------------------------------------- */ 

void Server::write_UDP_AckFIN( ) {

    int rc; 

    fd_set readSet; 
    FD_ZERO( &readSet ); 

    struct timeval timeout; 

    int count = 0; 
    while ( count < 10 ) {
        count++; 

        UDP_datagram *UDP_Hdr;
        server_hdr *hdr;

        UDP_Hdr = (UDP_datagram*) mBuf;

        if ( mSettings->mBufLen > (int) ( sizeof( UDP_datagram )
                                          + sizeof( server_hdr ) ) ) {
            Transfer_Info *stats = GetReport( mSettings->reporthdr );
            hdr = (server_hdr*) (UDP_Hdr+1);

            hdr->flags        = htonl( HEADER_VERSION1 );
            hdr->total_len1   = htonl( (long) (stats->TotalLen >> 32) );
            hdr->total_len2   = htonl( (long) (stats->TotalLen & 0xFFFFFFFF) );
            hdr->stop_sec     = htonl( (long) stats->endTime );
            hdr->stop_usec    = htonl( (long)((stats->endTime - (long)stats->endTime)
                                              * rMillion));
            hdr->error_cnt    = htonl( stats->cntError );
            hdr->outorder_cnt = htonl( stats->cntOutofOrder );
            hdr->datagrams    = htonl( stats->cntDatagrams );
            hdr->jitter1      = htonl( (long) stats->jitter );
            hdr->jitter2      = htonl( (long) ((stats->jitter - (long)stats->jitter) 
                                               * rMillion) );

        }

        // write data 
        write( mSettings->mSock, mBuf, mSettings->mBufLen ); 

        // wait until the socket is readable, or our timeout expires 
        FD_SET( mSettings->mSock, &readSet ); 
        timeout.tv_sec  = 1; 
        timeout.tv_usec = 0; 

        rc = select( mSettings->mSock+1, &readSet, NULL, NULL, &timeout ); 
        FAIL_errno( rc == SOCKET_ERROR, "select", mSettings ); 

        if ( rc == 0 ) {
            // select timed out 
            return; 
        } else {
            // socket ready to read 
            rc = read( mSettings->mSock, mBuf, mSettings->mBufLen ); 
            WARN_errno( rc < 0, "read" );
            if ( rc <= 0 ) {
                // Connection closed or errored
                // Stop using it.
                return;
            }
        } 
    } 

    fprintf( stderr, warn_ack_failed, mSettings->mSock, count ); 
} 
// end write_UDP_AckFIN 

