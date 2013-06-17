/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gurux.dlms.client;

import gurux.common.IGXMedia;
import gurux.common.ReceiveParameters;
import gurux.dlms.enums.RequestTypes;
import gurux.dlms.enums.InterfaceType;
import gurux.dlms.enums.DataType;
import gurux.dlms.enums.Authentication;
import gurux.dlms.GXDLMSClient;
import gurux.dlms.GXDLMSException;
import gurux.dlms.enums.ObjectType;
import gurux.dlms.manufacturersettings.*;
import gurux.dlms.objects.GXDLMSObject;
import gurux.dlms.objects.GXDLMSObjectCollection;
import gurux.net.GXNet;
import gurux.net.NetworkType;
import java.io.ByteArrayOutputStream;
import java.util.Date;

public class GXCommunicate
{
    IGXMedia Media;
    public boolean Trace = false;
    long ConnectionStartTime;
    GXManufacturer manufacturer;
    GXDLMSClient dlms;
    boolean iec;
    java.nio.ByteBuffer replyBuff;
    int WaitTime = 0;

    public GXCommunicate(int waitTime, gurux.dlms.GXDLMSClient dlms, GXManufacturer manufacturer, boolean iec, Authentication auth, String pw) throws Exception
    {             
        WaitTime = waitTime;
        this.dlms = dlms;
        this.manufacturer = manufacturer;
        this.iec = iec;
        dlms.setInterfaceType(manufacturer.getUseIEC47() ? InterfaceType.NET : InterfaceType.GENERAL);
        dlms.setUseLogicalNameReferencing(manufacturer.getUseLogicalNameReferencing());
        Object val = manufacturer.getAuthentication(Authentication.NONE).getClientID();
        long value = Long.parseLong(val.toString());
        value = value << 1 | 1;
        dlms.setClientID(GXManufacturer.convertTo(value, val.getClass()));
        GXServerAddress serv = manufacturer.getServer(HDLCAddressType.DEFAULT);        
        val = GXManufacturer.countServerAddress(serv.getHDLCAddress(), serv.getPhysicalAddress(), serv.getLogicalAddress());        
        dlms.setServerID(val);
        dlms.setAuthentication(auth);
        dlms.setPassword(pw);        
        System.out.println("Authentication: " + auth);
        System.out.println("ClientID: 0x" + Integer.toHexString(Integer.parseInt(dlms.getClientID().toString())));
        System.out.println("ServerID: 0x" + Integer.toHexString(Integer.parseInt(dlms.getServerID().toString())));
        if (dlms.getInterfaceType() == InterfaceType.NET)
        {
            replyBuff = java.nio.ByteBuffer.allocate(8 + 1024);
        }
        else
        {
            replyBuff = java.nio.ByteBuffer.allocate(100);
        }
    }

    void close() throws Exception
    {
        if (Media != null)
        {
            System.out.println("DisconnectRequest");
            readDLMSPacket(dlms.disconnectRequest());
            Media.close();
        }
    }

    /*
     * Read DLMS Data from the device.
     * If access is denied return null.
     */
    public byte[] readDLMSPacket(byte[] data) throws Exception
    {
        if (data == null || data.length == 0)
        {
            return null;
        }
        Object eop = (byte) 0x7E;
        //In network connection terminator is not used.
        if (dlms.getInterfaceType() == InterfaceType.NET && Media instanceof GXNet)
        {
            eop = null;
        }
        Integer pos = 0;
        boolean succeeded = false;
        ReceiveParameters<byte[]> p = new ReceiveParameters<>(byte[].class);
        p.setAllData(true);
        p.setEop(eop);
        p.setCount(5);
        p.setWaitTime(WaitTime);        
        synchronized (Media.getSynchronous())
        {
            while (!succeeded && pos != 3)
            {
                if (Trace)
                {   
                    System.out.println("<- " + GXDLMSClient.toHex(data));
                }
                Media.send(data, null);
                succeeded = Media.receive(p);
                if (!succeeded)
                {
                    //Try to read again...
                    if (++pos != 3)
                    {
                        System.out.println("Data send failed. Try to resend " + pos.toString() + "/3");
                        continue;
                    }   
                    throw new Exception("Failed to receive reply from the device in given time.");
                }
            }
            //Loop until whole Cosem packet is received.                
            while (!dlms.isDLMSPacketComplete(p.getReply()))
            {
                if (p.getEop() == null)
                {
                    p.setCount(1);
                }
                if (!Media.receive(p))
                {
                    throw new Exception("Failed to receive reply from the device in given time.");
                }
            }
        }
        if (Trace)
        {
            System.out.println("-> " + GXDLMSClient.toHex(p.getReply()));
        }
        Object[][] errors = dlms.checkReplyErrors(data, p.getReply());
        if (errors != null)
        {
            throw new GXDLMSException((int) errors[0][0]);
        }
        return p.getReply();       
    }
    
    /**
     * Opens connection again.
     * @throws Exception
     */
    void reOpen() throws Exception
    {
        /*
        if (manufacturer.getInactivityMode() == InactivityMode.REOPENACTIVE && serial != null && java.util.Calendar.getInstance().getTimeInMillis() - ConnectionStartTime > 40000)
        {
            String port = serial.getName();
            close();
            initializeSerial(port);
        } 
        * */
    }

    /**
     * Reads next data block.
     * @param data
     * @return
     * @throws Exception
     */
    byte[] readDataBlock(byte[] data) throws Exception
    {
        if (data.length == 0)
        {
            return new byte[0];
        }
        reOpen();
        byte[] reply = readDLMSPacket(data);
        ByteArrayOutputStream allData = new ByteArrayOutputStream();
        java.util.Set<RequestTypes> moredata = dlms.getDataFromPacket(reply, allData);
        //If there is nothing to send.
        if (allData == null)
        {
            return null;
        }
        int maxProgress = dlms.getMaxProgressStatus(allData);        
        int lastProgress = 0;
        float progress;
        while (!moredata.isEmpty())
        {
            while (moredata.contains(RequestTypes.FRAME))
            {                
                data = dlms.receiverReady(RequestTypes.FRAME);
                reply = readDLMSPacket(data);
                //Show progress.
                if (maxProgress != 1)
                {
                    progress = dlms.getCurrentProgressStatus(allData);
                    progress = progress / maxProgress * 80;
                    if (lastProgress != (int) progress)
                    {
                        if (Trace)
                        {
                            for(int pos = lastProgress; pos < (int) progress; ++pos)
                            {
                                System.out.print("-");
                            }
                        }
                        lastProgress = (int) progress;
                    }
                }                
                if (!dlms.getDataFromPacket(reply, allData).contains(RequestTypes.FRAME))
                {
                    moredata.remove(RequestTypes.FRAME);                    
                    break;
                }
            }
            reOpen();
            if (moredata.contains(RequestTypes.DATABLOCK))
            {             
                //Send Receiver Ready.
                data = dlms.receiverReady(RequestTypes.DATABLOCK);
                reply = readDLMSPacket(data);
                moredata = dlms.getDataFromPacket(reply, allData);
                //Show progress.
                if (maxProgress != 1)
                {
                    progress = dlms.getCurrentProgressStatus(allData);
                    if (Trace)
                    {
                        progress = progress / maxProgress * 80;
                        if (lastProgress != (int) progress)
                        {
                            for(int pos = lastProgress; pos < (int) progress; ++pos)
                            {
                                System.out.print("+");
                            }
                            lastProgress = (int) progress;
                        }
                    }
                 }
            }
        }
        if (maxProgress > 1)
        {
            if (Trace)
            {
                System.out.println("");
            }
        }
        return allData.toByteArray();
    }

    /**
     * Initializes serial connection.
     * @param port
     * @throws Exception
     */
    /*
    void initializeSerial(String port) throws Exception
    {        
        Enumeration portList = CommPortIdentifier.getPortIdentifiers();
	while (portList.hasMoreElements())
        {
	    CommPortIdentifier portId = (CommPortIdentifier) portList.nextElement();
            System.out.println(portId.getName());
	    if (portId.getPortType() == CommPortIdentifier.PORT_SERIAL &&
                    portId.getName().equalsIgnoreCase(port))
            {
                serial = (SerialPort) portId.open(this.getClass().getName(), 2000);
                if (iec)
                {
                    serial.setSerialPortParams(300, SerialPort.DATABITS_7,
					   SerialPort.STOPBITS_1,
					   SerialPort.PARITY_EVEN);
                }
                else
                {
                    serial.setSerialPortParams(9600, SerialPort.DATABITS_8,
					   SerialPort.STOPBITS_1,
					   SerialPort.PARITY_NONE);
                }
            }
        }
	if (serial == null)
        {
	    throw new Exception("Port " + port + " not found.");
	}
        if (iec)
        {
            int Terminator = '\n';
            String dataStr = "/?!\r\n";
            String replyStr = new String(this.send(dataStr.getBytes("ASCII"), Terminator));
            //If echo is used.
            if (replyStr.equals(dataStr))
            {
                replyStr = new String(this.send(dataStr.getBytes("ASCII"), Terminator));
            }
            System.out.println("IEC received: " + replyStr);
            if (replyStr.length() == 0 || replyStr.charAt(0) != '/')
            {
                throw new Exception("Invalid responce.");
            }
            String manufactureID = replyStr.substring(1, 3);
            if (manufacturer.getIdentification().compareToIgnoreCase(manufactureID) != 0)
            {
                throw new Exception("Manufacturer " + manufacturer.getIdentification() + " expected but " + manufactureID + " found.");
            }
            char baudrate = replyStr.charAt(4);
            int bitrate = 0;
            switch (baudrate)
            {
                case '0':
                    bitrate = 300;
                    break;
                case '1':
                    bitrate = 600;
                    break;
                case '2':
                    bitrate = 1200;
                    break;
                case '3':
                    bitrate = 2400;
                    break;
                case '4':
                    bitrate = 4800;
                    break;
                case '5':
                    bitrate = 9600;
                    break;
                case '6':
                    bitrate = 19200;
                    break;
                default:
                    throw new Exception("Unknown baud rate.");
            }
            System.out.println("Bitrate is : " + bitrate);
            //Send ACK
            //Send Protocol control character
            byte controlCharacter = (byte)'2';// "2" HDLC protocol procedure (Mode E)
            //Send Baudrate character
            //Mode control character
            byte ModeControlCharacter = (byte)'2';//"2" //(HDLC protocol procedure) (Binary mode)
            //Set mode E.
            byte[] tmp = new byte[] { 0x06, controlCharacter, (byte)baudrate, ModeControlCharacter, 13, 10 };
            this.send(tmp, null);
            serial.setSerialPortParams(bitrate, SerialPort.DATABITS_8,
                                       SerialPort.STOPBITS_1,
                                       SerialPort.PARITY_NONE);
        }
        ConnectionStartTime = java.util.Calendar.getInstance().getTimeInMillis();
        byte[] reply = null;
        byte[] data = dlms.SNRMRequest();
        if (data != null)
        {
            reply = readDLMSPacket(data);
            //Has server accepted client.
            dlms.parseUAResponse(reply);
        }
        //Generate AARQ request.
        //Split requests to multible packets if needed.
        //If password is used all data might not fit to one packet.
        for (byte[] it : dlms.AARQRequest(null))
        {            
            reply = readDLMSPacket(it);
        }
        //Parse reply.
        dlms.parseAAREResponse(reply);
    }
    * */

    /**
     * Initializes TCP/IP connection.
     * @param host
     * @param port
     * @throws Exception
     */
    void initializeSocket(String host, int port) throws Exception
    {
        close();
        Media = new GXNet(NetworkType.TCP, host, port);
        Media.open();
        byte[] reply = null;
        byte[] data = dlms.SNRMRequest();
        //This is ignored if IEC 62056-47 is used.
        if (data.length != 0)
        {
            reply = readDLMSPacket(data);
            //Has server accepted client.
            dlms.parseUAResponse(reply);
            //Allocate buffer to same size as transmit buffer of the meter.
            //Size of replyBuff is payload and frame (Bop, EOP, crc).            
            int size = (int) ((((Number)dlms.getLimits().getMaxInfoTX()).intValue() & 0xFFFFFFFFL) + 40);
            replyBuff = java.nio.ByteBuffer.allocate(size);
        }
        //Generate AARQ request.
        //Split requests to multible packets if needed.
        //If password is used all data might not fit to one packet.
        for (byte[] it : dlms.AARQRequest(null))
        {
            reply = readDLMSPacket(it);
        }
        //Parse reply.
        dlms.parseAAREResponse(reply);
    }   

    /**
     * Reads selected DLMS object with selected attribute index.
     * @param item
     * @param attributeIndex
     * @return
     * @throws Exception
     */
    public Object readObject(GXDLMSObject item, int attributeIndex) throws Exception
    {
        byte[] data = dlms.read(item.getName(), item.getObjectType(), attributeIndex)[0];
        data = readDataBlock(data);
        return dlms.getValue(data, item.getObjectType(), item.getLogicalName(), attributeIndex);
    }

    /*
     * Returns columns of profile Generic.
     */
    public GXDLMSObjectCollection GetColumns(GXDLMSObject pg) throws Exception
    {
        Object entries = readObject(pg, 7);
        GXObisCode code = manufacturer.getObisCodes().findByLN(pg.getObjectType(), pg.getLogicalName(), null);
        if (code != null)
        {
            System.out.println("Reading Profile Generic: " + pg.getLogicalName() + " " + code.getDescription() + " entries:" + entries.toString());
        }
        else
        {
            System.out.println("Reading Profile Generic: " + pg.getLogicalName() + " entries:" + entries.toString());
        }
        byte[] data = dlms.read(pg.getName(), pg.getObjectType(), 3)[0];
        data = readDataBlock(data);
        return dlms.parseColumns(data);
    }

    /**
     * Read Profile Generic's data by entry start and count.
     * @param pg
     * @param index
     * @param count
     * @return
     * @throws Exception
     */
    public Object[] readRowsByEntry(GXDLMSObject pg, GXDLMSObjectCollection columns, int index, int count) throws Exception
    {
        byte[] data = dlms.readRowsByEntry(pg.getName(), index, count);
        data = readDataBlock(data);        
        Object[] rows = (Object[]) dlms.getValue(data);
        if (columns != null && rows.length != 0 && dlms.getObisCodes() != null)
        {
            Object[] row = (Object[]) rows[0];
            if (columns.size() != row.length)
            {
                throw new Exception("Columns count do not mach.");
            }
            for (int pos = 0; pos != columns.size(); ++pos)
            {
                if (row[pos] instanceof byte[])
                {
                    DataType type = DataType.NONE;
                    //Find Column type
                    GXDLMSObject col = columns.get(pos);
                    GXObisCode code = dlms.getObisCodes().findByLN(col.getObjectType(), col.getLogicalName(), null);
                    if (code != null)
                    {
                        GXDLMSAttributeSettings att = code.getAttributes().find(col.getSelectedAttributeIndex());
                        if (att != null)
                        {
                            type = att.getUIType();                            
                        }
                        if (type == DataType.NONE)
                        {
                            int attributeIndex = col.getSelectedAttributeIndex();
                            if((col.getObjectType() == ObjectType.CLOCK && attributeIndex == 2) ||
                                (col.getObjectType() == ObjectType.EXTENDED_REGISTER && attributeIndex == 5))
                            {
                                type = DataType.DATETIME;
                            }
                        }
                        if (type != DataType.NONE)
                        {
                            for(Object it : rows)
                            {
                                Object[] cells = (Object[]) it;
                                cells[pos] = GXDLMSClient.changeType((byte[]) cells[pos], type);
                            }
                        }
                    }
                }
            }
        }
        return rows;
    }

    /**
     * Read Profile Generic's data by range (start and end time).
     * @param pg
     * @param columns
     * @param start
     * @param end
     * @return
     * @throws Exception
     */
    public Object[] readRowsByRange(GXDLMSObject pg, GXDLMSObjectCollection columns, Date start, Date end) throws Exception
    {
        byte[] data = dlms.readRowsByRange(pg.getName(), columns.get(0).getLogicalName(), pg.getObjectType(), columns.get(0).getVersion(), start, end);
        data = readDataBlock(data);
        Object[] rows = (Object[]) dlms.getValue(data);
        if (rows.length != 0 && dlms.getObisCodes() != null)
        {
            Object[] row = (Object[]) rows[0];
            if (columns.size() != row.length)
            {
                throw new Exception("Columns count do not mach.");
            }
            for (int pos = 0; pos != columns.size(); ++pos)
            {
                if (row[pos] instanceof byte[])
                {
                    DataType type = DataType.NONE;
                    //Find Column type
                    GXDLMSObject col = columns.get(pos);
                    GXObisCode code = dlms.getObisCodes().findByLN(col.getObjectType(), col.getLogicalName(), null);
                    if (code != null)
                    {
                        GXDLMSAttributeSettings att = code.getAttributes().find(col.getSelectedAttributeIndex());
                        if (att != null)
                        {
                            type = att.getUIType();                            
                        }
                        if (type == DataType.NONE)
                        {
                            int attributeIndex = col.getSelectedAttributeIndex();
                            if((col.getObjectType() == ObjectType.CLOCK && attributeIndex == 2) ||
                                (col.getObjectType() == ObjectType.EXTENDED_REGISTER && attributeIndex == 5))
                            {
                                type = DataType.DATETIME;
                            }
                        }
                        if (type != DataType.NONE)
                        {
                            for(Object it : rows)
                            {
                                Object[] cells = (Object[]) it;
                                cells[pos] = GXDLMSClient.changeType((byte[]) cells[pos], type);
                            }
                        }
                    }
                }
            }
        }
        return rows;
    }
}
