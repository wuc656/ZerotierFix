package net.kaaass.zerotierfix.service;

import android.util.Log;

import com.zerotier.sdk.Node;
import com.zerotier.sdk.PacketSender;
import com.zerotier.sdk.ResultCode;

import net.kaaass.zerotierfix.util.DebugLog;

import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

// TODO: clear up
public class UdpCom implements PacketSender, Runnable {
    private static final String TAG = "UdpCom";
    private Node node;
    private final DatagramChannel svrChannel;
    private final ZeroTierOneService ztService;

    UdpCom(ZeroTierOneService zeroTierOneService, DatagramChannel datagramChannel) {
        this.svrChannel = datagramChannel;
        this.ztService = zeroTierOneService;
    }

    public void setNode(Node node2) {
        this.node = node2;
    }

    @Override // com.zerotier.sdk.PacketSender
    public int onSendPacketRequested(long j, InetSocketAddress inetSocketAddress, byte[] bArr, int i) {
        if (this.svrChannel == null) {
            Log.e(TAG, "Attempted to send packet on a null socket");
            return -1;
        }
        try {
            DebugLog.d(TAG, "onSendPacketRequested: Sent " + packet.length + " bytes to " + inetSocketAddress.toString());
            this.svrChannel.send(bArr, inetSocketAddress);
            return 0;
        } catch (Exception unused) {
            return -1;
        }
    }

    public void run() {
        Log.d(TAG, "UDP Listen Thread Started.");
        try {
            long[] jArr = new long[1];
            ByteBuffer buf = ByteBuffer.allocateDirect(16384);
            while (this.svrChannel.isOpen()) {
                jArr[0] = 0;
                try {
                    SocketAddress recvSockAddr = this.svrChannel.receive(buf);
                    buf.flip();
                    if (buf.remaining() > 0) {
                        DebugLog.d(TAG, "Got " + buf.remaining() + " Bytes From: " + recvSockAddr);
                        byte[] data = new byte[buf.remaining()];
                        buf.get(data);
                        ResultCode processWirePacket = this.node.processWirePacket(System.currentTimeMillis(), -1, (InetSocketAddress) recvSockAddr, data, jArr);
                        if (processWirePacket != ResultCode.RESULT_OK) {
                            Log.e(TAG, "processWirePacket returned: " + processWirePacket.toString());
                            this.ztService.shutdown();
                        }
                        this.ztService.setNextBackgroundTaskDeadline(jArr[0]);
                    }
                    buf.clear();
                } catch (SocketTimeoutException ignored) {
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        Log.d(TAG, "UDP Listen Thread Ended.");
    }
}
