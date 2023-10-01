package org.embulk.parser.seqfile.column;

import static org.junit.Assert.assertEquals;

import java.io.DataInput;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.embulk.spi.PageBuilder;
import org.junit.Test;

public class WritableColumnTest {

    private static class WritableColumnMock extends WritableColumn {

        public WritableColumnMock() {
            super(null, null, null);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(PageBuilder pageBuilder) {
            throw new UnsupportedOperationException();
        }

        public String mockZoneId;

        @Override
        protected String getZoneIdString() {
            return mockZoneId;
        }
    }

    @Test
    public void getZoneId() {
        {
            WritableColumnMock target = new WritableColumnMock();
            target.mockZoneId = "UTC";

            ZoneId zoneId = target.getZoneId();
            assertEquals("UTC", zoneId.getId());
        }
        {
            WritableColumnMock target = new WritableColumnMock();
            target.mockZoneId = "Asia/Tokyo";

            ZoneId zoneId = target.getZoneId();
            assertEquals("Asia/Tokyo", zoneId.getId());
        }
        {
            WritableColumnMock target = new WritableColumnMock();
            target.mockZoneId = "JST";

            ZoneId zoneId = target.getZoneId();
            assertEquals("Asia/Tokyo", zoneId.getId());
        }
    }

    @Test
    public void getZoneOffset() {
        {
            WritableColumnMock target = new WritableColumnMock();
            target.mockZoneId = "UTC";

            ZoneOffset zoneOffset = target.getZoneOffset();
            assertEquals("Z", zoneOffset.getId());
        }
        {
            WritableColumnMock target = new WritableColumnMock();
            target.mockZoneId = "Asia/Tokyo";

            ZoneOffset zoneOffset = target.getZoneOffset();
            assertEquals("+09:00", zoneOffset.getId());
        }
        {
            WritableColumnMock target = new WritableColumnMock();
            target.mockZoneId = "JST";

            ZoneOffset zoneOffset = target.getZoneOffset();
            assertEquals("+09:00", zoneOffset.getId());
        }
    }
}
