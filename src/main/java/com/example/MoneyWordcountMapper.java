/*
 * WordcountMapper.java
 *
 * Copyright 2014 Luca Menichetti <meniluca@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 *
 */

package com.example;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class MoneyWordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public static final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {


        Type listType = new TypeToken<ArrayList<Question>>() {}.getType();
        List<Question> jsonObjects = new Gson().fromJson(line.toString(), listType);

        for (Question word : jsonObjects) {
            // check for null
            String value = word.getValue() == null ? "Kein Wert" : word.getValue();
            // remove , and $ signs
            value = value.replaceAll("\\$|,", "").trim();
            context.write(new Text(value), ONE);
        }
    }
}
