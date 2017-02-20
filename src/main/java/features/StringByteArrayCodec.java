package features;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.protocol.LettuceCharsets;

import static java.nio.charset.CoderResult.OVERFLOW;


public class StringByteArrayCodec implements RedisCodec<String, byte[]>{

    private Charset charset;
    private CharsetDecoder decoder;
    private CharBuffer chars;
    
    public StringByteArrayCodec() {
        charset = LettuceCharsets.UTF8;
        decoder = charset.newDecoder();
        chars = CharBuffer.allocate(1024);
    }
    
//    public static final ByteArrayCodec INSTANCE = new ByteArrayCodec();
    private static final byte[] EMPTY = new byte[0];

    @Override
    public String decodeKey(ByteBuffer bytes) {
        return decode(bytes);
    }

    @Override
    public byte[] decodeValue(ByteBuffer bytes) {
        return getBytes(bytes);
    }

    @Override
    public ByteBuffer encodeKey(String key) {
        return encode(key);
    }

    @Override
    public ByteBuffer encodeValue(byte[] value) {

        if(value == null){
            return ByteBuffer.wrap(EMPTY);
        }

        return ByteBuffer.wrap(value);
    }

    private static byte[] getBytes(ByteBuffer buffer) {
        byte[] b = new byte[buffer.remaining()];
        buffer.get(b);
        return b;
    }
    
    private synchronized String decode(ByteBuffer bytes) {
        chars.clear();
        bytes.mark();

        decoder.reset();
        while (decoder.decode(bytes, chars, true) == OVERFLOW || decoder.flush(chars) == OVERFLOW) {
            chars = CharBuffer.allocate(chars.capacity() * 2);
            bytes.reset();
        }

        return chars.flip().toString();
    }
    
    private ByteBuffer encode(String string) {
        if (string == null) {
            return ByteBuffer.wrap(EMPTY);
        }

        return charset.encode(string);
    }
}
