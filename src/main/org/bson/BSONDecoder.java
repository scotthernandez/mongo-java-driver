// BSONDecoder.java

package org.bson;

import static org.bson.BSON.*;

import java.io.*;

import org.bson.io.*;
import org.bson.types.*;

public class BSONDecoder {
    
    public BSONObject readObject( byte[] b ){
        try {
            return readObject( new ByteArrayInputStream( b ) );
        }
        catch ( IOException ioe ){
            throw new RuntimeException( "should be impossible" , ioe );
        }
    }

    public BSONObject readObject( InputStream in )
        throws IOException {
        BasicBSONCallback c = new BasicBSONCallback();
        decode( in , c );
        return (BSONObject)c.get();
    }

    public int decode( byte[] b , BSONCallback callback ){
        try {
            return decode( new Input( new ByteArrayInputStream(b) ) , callback );
        }
        catch ( IOException ioe ){
            throw new RuntimeException( "should be impossible" , ioe );
        }
    }


    public int decode( InputStream in , BSONCallback callback )
        throws IOException {
        return decode( new Input( in ) , callback );
    }
    
    int decode( Input in  , BSONCallback callback )
        throws IOException {

        if ( _in != null || _callback != null )
            throw new IllegalStateException( "not ready" );
        
        _in = in;
        _callback = callback;
        
        try {
            return decode();
        }
        finally {
            _in = null;
            _callback = null;
        }
    }
    
    int decode()
        throws IOException {
    	//
    	// We already read four bytes for length
        final int start = _in.getBytesRead() - 4;
        
        _callback.objectStart();
        while ( decodeElement() );
        _callback.objectDone();
        
        final int read = _in.getBytesRead() - start;

        if ( read != _in._length ) {
            //throw new IllegalArgumentException( "bad data.  lengths don't match " + read + " != " + len );
        }

        return _in._length;
    }
    
    boolean decodeElement()
        throws IOException {

        final byte type = _in.read();
        if ( type == EOO )
            return false;
        
        String name = _in.readCStr();
        
        switch ( type ){
        case NULL:
            _callback.gotNull( name ); 
            break;
            
        case UNDEFINED:
            _callback.gotUndefined( name ); 
            break;

        case BOOLEAN:
            _callback.gotBoolean( name , _in.read() > 0 );
            break;

        case NUMBER:
            _callback.gotDouble( name , _in.readDouble() );
            break;
	    
        case NUMBER_INT:
            _callback.gotInt( name , _in.readInt() );
            break;

        case NUMBER_LONG:
            _callback.gotLong( name , _in.readLong() );
            break;	    

            
        case SYMBOL:
            _callback.gotSymbol( name , _in.readUTF8String() );
            break;
            

        case STRING:
            _callback.gotString( name , _in.readUTF8String() );
            break;

        case OID:
            _callback.gotObjectId( name , new ObjectId( _in.readInt() , _in.readInt() , _in.readInt() ) );
            break;
            
        case REF:
            _in.readInt();  // length of ctring that follows
            String ns = _in.readCStr();
            ObjectId theOID = new ObjectId( _in.readInt() , _in.readInt() , _in.readInt() );
            _callback.gotDBRef( name , ns , theOID );
            break;
            
        case DATE:
            _callback.gotDate( name , _in.readLong() );
            break;
            
        case REGEX:
            _callback.gotRegex( name , _in.readCStr() , _in.readCStr() );
            break;

        case BINARY:
            _binary( name );
            break;
            
        case CODE:
            _callback.gotCode( name , _in.readUTF8String() );
            break;

        case CODE_W_SCOPE:
            _in.readInt();
            _callback.gotCodeWScope( name , _in.readUTF8String() , _readBasicObject() );

            break;

        case ARRAY:
            _in.readInt();  // total size - we don't care....

            _callback.arrayStart( name );
            while ( decodeElement() );
            _callback.arrayDone();

            break;
            
            
        case OBJECT:
            _in.readInt();  // total size - we don't care....
            
            _callback.objectStart( name );
            while ( decodeElement() );
            _callback.objectDone();

            break;
            
        case TIMESTAMP:
            int i = _in.readInt();
            int time = _in.readInt();
            _callback.gotTimestamp( name , time , i );
            break;

        case MINKEY:
            _callback.gotMinKey( name );
            break;

        case MAXKEY:
            _callback.gotMaxKey( name );
            break;

        default:
            throw new UnsupportedOperationException( "BSONDecoder doesn't understand type : " + type + " name: " + name  );
        }
        
        return true;
    }

    void _binary( String name )
        throws IOException {
        final int totalLen = _in.readInt();
        final byte bType = _in.read();
        
        switch ( bType ){
        case B_GENERAL: {
            final byte[] data = new byte[totalLen];
            _in.fill( data );
            _callback.gotBinaryArray( name , data );
            return;
        }
        case B_BINARY: {
            final int len = _in.readInt();
            if ( len + 4 != totalLen )
                throw new IllegalArgumentException( "bad data size subtype 2 len: " + len + " totalLen: " + totalLen );
            
            final byte[] data = new byte[len];
            _in.fill( data );
            _callback.gotBinaryArray( name , data );
            return;
        }
        case B_UUID:
            if ( totalLen != 16 )
                throw new IllegalArgumentException( "bad data size subtype 3 len: " + totalLen + " != 16");
            
            long part1 = _in.readLong();
            long part2 = _in.readLong();
            _callback.gotUUID(name, part1, part2);
            return;	
        }
        
        byte[] data = new byte[totalLen];
        _in.fill( data );

        _callback.gotBinary( name , bType , data );
    }
    
    Object _readBasicObject()
        throws IOException {
        _in.readInt();
        
        BSONCallback save = _callback;
        BSONCallback _basic = _callback.createBSONCallback();
        _callback = _basic;
        _basic.reset();
        _basic.objectStart(false);

        while( decodeElement() );
        _callback = save;
        return _basic.get();
    }
    
    class Input {
    	/**
    	 * Maximum size of readahead. This ensures that we copy in memory at most
    	 * readahead bytes if the buffer does not contain enough continuous bytes.
    	 * Must be lower or equal than size of _charBuffer to prevent a buffer overflow.
    	 */
    	final private static int MAX_READAHEADSIZE = 512;

    	Input( final InputStream in ) 
    		throws IOException {
            _in = in;
            _read = 0;
            //
            // Limit Buffer to only read 4 bytes for the real length
            _length = 4;
            _length = readInt();
        }
        /**
         * Ensures that a continuous block of bytes is loaded to the buffer. Its responsibility to consume
         * the complete block.
         * 
         * @param blockSize
         * @throws IOException
         */
        void ensureContinuousBlock(int blockSize) 
        	throws IOException {
        	//
        	// Enough bytes already loaded?
        	if(_o + blockSize <= _l)
        		return;
        	
        	final int remaining = _l - _o;
        	//
        	// Is buffer large enough for block?
        	if(blockSize < _random.length) {        		        	
            	//
            	// copy the rest in the buffer to the front
            	System.arraycopy(_random, _o, _random, 0, remaining);        		
        	}
        	else {
        		//
        		// Allocate a larger buffer
        		final byte largerBuffer[] = new byte[blockSize + MAX_READAHEADSIZE];
        		//
        		// copy the rest of the old buffer to the front of the new
            	System.arraycopy(_random, _o, largerBuffer, 0, remaining);
            	//
            	// swap the buffers
            	_random = largerBuffer;
        	}
        	//
        	// Increase the numbers of bytes by all processed bytes (offset with current buffer)
        	// Buffer is now aligned with the front
        	_read += _o;
        	
        	_o = 0;
        	_l = remaining;
        	//
        	// Calculate possible readahead. It is not allowed to read beyond the end of the current object (_length)
        	final int bytesTillEnd = _length - _read - _l;
        	final int readahead = Math.min(Math.min(MAX_READAHEADSIZE, _random.length - remaining), bytesTillEnd);
        	
        	int wanted = Math.max(readahead, blockSize - remaining);
        	
        	while(wanted > 0 && _l < blockSize) {
        		//
        		// Read as much as we wanted at the end of the buffer
            	int rd = _in.read(_random, _l, wanted);
            	//
            	// EOS reached?
            	if(rd < 0)
            		break;
            	//
            	// Increase end and reduced wanted by bytes read from InputStream
            	_l = _l + rd;
            	wanted -=rd;
        	}
        	//
        	// Ups, we were not able to read enough bytes from stream
        	if(_l < blockSize) {
        		throw new RuntimeException("end of stream reached");
        	}
        }

        /**
         * Reads an integer.
         * 
         * @return
         * @throws IOException
         */
        final int readInt()
            throws IOException {
        	//
        	// All integers are 4 bytes
        	ensureContinuousBlock(4);
            //
            // Code copied from java.io.Bits
        	return 
        		((_random[_o++] & 0xFF) << 0) +
 	       		((_random[_o++] & 0xFF) << 8) +
 	       		((_random[_o++] & 0xFF) << 16) +
 	       		((_random[_o++]) << 24);
        }
        /**
         * Reads a long.
         * 
         * @return
         * @throws IOException
         */
        long readLong()
        	throws IOException {
        	//
        	// All longs are 8 bytes
	        ensureContinuousBlock(8);     
            //
            // Code copied from java.io.Bits	        
	    	return ((_random[_o++] & 0xFFL) << 0) +
		       ((_random[_o++] & 0xFFL) << 8) +
		       ((_random[_o++] & 0xFFL) << 16) +
		       ((_random[_o++] & 0xFFL) << 24) +
		       ((_random[_o++] & 0xFFL) << 32) +
		       ((_random[_o++] & 0xFFL) << 40) +
		       ((_random[_o++] & 0xFFL) << 48) +
		       (((long) _random[_o++]) << 56);
        }
        /**
         * Simply read a double
         * 
         * @return
         * @throws IOException
         */
        double readDouble()
            throws IOException {
            return Double.longBitsToDouble( readLong() );
        }
        /**
         * Read the next byte from stream.
         * 
         * @return
         * @throws IOException
         */
        byte read()
        	throws IOException {
        	//
        	// Ensure that one byte can be read
        	ensureContinuousBlock(1);
        	//
        	// Simply return the byte
        	return _random[_o++];
        }

        void fill( byte b[] )
            throws IOException {
            fill( b , b.length );
        }

        void fill( byte b[] , int len )
        	throws IOException {
	        //
	        // Take the remaining bytes from the buffer
	        int remaining = _l - _o;
	        //
	        // Did we alread read enough bytes?
	        if(remaining >= len) {
	        	System.arraycopy(_random, _o, b, 0, len);	        	
	        	_o += len;
	        	
	        	return;
	        }
	        //
	        // Take the complete remaining bytes from buffer
	        if(remaining > 0) {
	        	System.arraycopy(_random, _o, b, 0, remaining);
	        	//
	        	// Reduced needed bytes
	        	len -= remaining;
	        	//
	        	// leave it up to the next ensure a continuous block
	        	_o = _l;
	        }
	        //
	        // Read the rest direct from the InputStream
	        while ( len > 0 ) {
	            final int bytesRead = _in.read( b , remaining , len );
	        	//
	        	// Reduced needed bytes	            
	            len -= bytesRead;
	            //
	            // Increase the number of read bytes because we reading directly from _in
	            _read += bytesRead;

	            remaining += bytesRead;
	        }
	    }
        /**
         * Read a multibyte character with the first given as parameter <code>c1</code>.
         * 
         * @param c1
         * @return
         * @throws IOException
         */
        char readMultiByte(int c1) 
        	throws IOException {
            switch (c1 >> 4) {
                case 12: 
                case 13: {
                	//
                	// We need at least one byte for the character and one for the null to terminate
                	ensureContinuousBlock(2);
                	//
                	// Read next byte and check for correctness
                    final int c2 = _random[_o++];
                    
                    if ((c2 & 0xC0) != 0x80)
                    	return '\uFFFD';
                    
                    return (char)(((c1 & 0x1F) << 6) | (c2 & 0x3F));
                }
                case 14: {
                	//
                	// We need at least two bytes for the character and one for the null to terminate
                	ensureContinuousBlock(3);
                	//
                	// Read next bytes and check for correctness                	
                	final int c2 = _random[_o++];
                	final int c3 = _random[_o++];
                	
                	if (((c2 & 0xC0) != 0x80) || ((c3 & 0xC0) != 0x80)) 
                    	return '\uFFFD';
                	
                	return (char)(((c1 & 0x0F) << 12) | ((c2 & 0x3F) << 6)  | ((c3 & 0x3F) << 0));
                }	
                default:
                	return '\uFFFD';
            }
        }
        /**
         * Read an null terminated string in UTF8 from {@link InputStream}. 
         * We assume that null terminated strings have small lengths and are mostly ascii.
         * 
         * @return
         * @throws IOException
         */

        String readCStr()
            throws IOException {
            
            // short circuit 1 byte strings
            {
                _random[0] = read();
                if ( _random[0] == 0 )
                    return "";
                
                _random[1] = read();
                if ( _random[1] == 0 ){
                    String out = ONE_BYTE_STRINGS[_random[0]];
                    if ( out != null )
                        return out;
                    return new String( _random , 0 , 1 , "UTF-8" );
                }

                _stringBuffer.reset();
                _stringBuffer.write( _random[0] );
                _stringBuffer.write( _random[1] );

            }


            while ( true ){
                byte b = read();
                if ( b == 0 )
                    break;
                _stringBuffer.write( b );
            }

            String out = null;
            try {
                out = _stringBuffer.asString( "UTF-8" );
            }
            catch ( UnsupportedOperationException e ){
                throw new RuntimeException( "impossible" , e );
            }
            _stringBuffer.reset();
            return out;
        }
        /**
         * Read an UTF8-String from {@link InputStream}. 
         * 
         * @return
         * @throws IOException
         */
        String readUTF8String()
            throws IOException {
        	//
        	// Read size and ensure that the complete string is in the buffer
            final int size = readInt();
            if ( size < 0 || size > ( 3 * 1024 * 1024 ) )
                throw new RuntimeException( "bad string size: " + size );
            
            ensureContinuousBlock(size);
            //
            // Start of the string is the current pointer in buffer
            final int startOfString = _o;
            //
            // Increase offset by size of string
            _o += size;

            try {
                return new String( _random, startOfString , size - 1 , "UTF-8" );
            }
            catch ( java.io.UnsupportedEncodingException uee ){
                throw new RuntimeException( "impossible" , uee );
            }
        }
        /**
         * Returns the number of bytes read so far.
         * 
         * @return
         */
        int getBytesRead() {
        	return _read + _o;
        }
        
        int _o;
        int _l;
        int _read;
        
        final InputStream _in;
        int _length;
    }

    private Input _in;
    private BSONCallback _callback;
    private byte[] _random = new byte[1024]; // has to be used within a single function

    private PoolOutputBuffer _stringBuffer = new PoolOutputBuffer();

    static final String[] ONE_BYTE_STRINGS = new String[128];
    static void _fillRange( byte min, byte max ){
        while ( min < max ){
            String s = "";
            s += (char)min;
            ONE_BYTE_STRINGS[(int)min] = s;
            min++;
        }
    }
    static {
        _fillRange( (byte)'0' , (byte)'9' );
        _fillRange( (byte)'a' , (byte)'z' );
        _fillRange( (byte)'A' , (byte)'Z' );
    }
}
