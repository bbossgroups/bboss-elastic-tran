package org.frameworkset.elasticsearch.client;
/**
 * Copyright 2023 bboss
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.junit.Test;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/5/25
 * @author biaoping.yin
 * @version 1.0
 */
public class TestLocalTime {
    @Test
    public void contextLoads() throws ParseException {

        DateTimeFormatter ISO_LOCAL_DATE = new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                .optionalStart().appendLiteral('/').optionalEnd()
                .optionalStart().appendLiteral('-').optionalEnd()
                .optionalStart().appendValue(ChronoField.MONTH_OF_YEAR, 2)
                .optionalStart().appendLiteral('/').optionalEnd()
                .optionalStart().appendLiteral('-').optionalEnd()
                .optionalStart().appendValue(ChronoField.DAY_OF_MONTH, 2)
                .toFormatter();

        DateTimeFormatter ISO_LOCAL_TIME = new DateTimeFormatterBuilder()
                .appendValue(ChronoField.HOUR_OF_DAY, 2)
                .optionalStart().appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                .optionalStart().appendLiteral(':').appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                .optionalStart().appendZoneId()
                .toFormatter();

        DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
                .append(ISO_LOCAL_DATE)
                .optionalStart().appendLiteral(' ').optionalEnd()
                .optionalStart().appendLiteral('T').optionalEnd()
                .optionalStart().appendOptional(ISO_LOCAL_TIME).optionalEnd()
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                .toFormatter(Locale.SIMPLIFIED_CHINESE);

        Stream.of("2015-05-09T00:10:23.934596635Z",
                "2015-05-09 00:10:23.123456789UTC",
                "2015/05/09 00:10:23.123456789",
                "2015-05-09 00:10:23.12345678",
                "2015/05/09 00:10:23.1234567",
                "2015-05-09T00:10:23.123456",
                "2015-05-09 00:10:23.12345",
                "2015/05-09T00:10:23.1234",
                "2015-05-09 00:10:23.123",
                "2015-05-09 00:10:23.12",
                "2015-05-09 00:10:23.1",
                "2015-05-09 00:10:23",
                "2015-05-09 00:10",
                "2015-05-09 01",
                "2015-05-09"
        ).forEach(s -> {
            LocalDateTime date = LocalDateTime.parse(s, dateTimeFormatter);
            System.out.println(s + " localdate==> " + date);

            System.out.println(s + " date==> " + par(s));
        });
    }

    private Date par(String value){
        DateTimeFormatter isoLocalDate = new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                .optionalStart().appendLiteral('/').optionalEnd()
                .optionalStart().appendLiteral('-').optionalEnd()
                .optionalStart().appendValue(ChronoField.MONTH_OF_YEAR, 2)
                .optionalStart().appendLiteral('/').optionalEnd()
                .optionalStart().appendLiteral('-').optionalEnd()
                .optionalStart().appendValue(ChronoField.DAY_OF_MONTH, 2)
                .toFormatter();

        DateTimeFormatter isoLocalTime = new DateTimeFormatterBuilder()
                .appendValue(ChronoField.HOUR_OF_DAY, 2)
                .optionalStart().appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                .optionalStart().appendLiteral(':').appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                .optionalStart().appendZoneId()
                .toFormatter();

        DateTimeFormatter dateFormat = new DateTimeFormatterBuilder()
                .append(isoLocalDate)
                .optionalStart().appendLiteral(' ').optionalEnd()
                .optionalStart().appendLiteral('T').optionalEnd()
                .optionalStart().appendOptional(isoLocalTime).optionalEnd()
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                .toFormatter(Locale.SIMPLIFIED_CHINESE);

        LocalDateTime parse = LocalDateTime.parse((String) value, dateFormat);
        return Date.from(parse.atZone(ZoneId.systemDefault()).toInstant());
    }
}
