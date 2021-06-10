/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.jdbc.model.join;

import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.j_spaces.jdbc.builder.range.Range;
import net.sf.jsqlparser.statement.select.Join;

import java.util.Objects;

public class JoinInfo {

    private final QueryColumn leftColumn;
    private final QueryColumn rightColumn;
    private final JoinType joinType;
    private Range range;

    public JoinInfo(QueryColumn leftColumn, QueryColumn rightColumn, JoinType joinType) {
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
        this.joinType = joinType;
    }

    public boolean checkJoinCondition(){
        Object rightValue = rightColumn.getCurrentValue();
        Object leftValue = leftColumn.getCurrentValue();
        if(joinType.equals(JoinType.INNER)) {
            return rightValue != null && leftValue != null && Objects.equals(leftValue, rightValue);
        }
        if(range != null){
            if(range.getPath().equals(rightColumn.getName()))
                return range.getPredicate().execute(rightValue);
            return range.getPredicate().execute(leftValue);
        }
        return true;
    }

    public QueryColumn getLeftColumn() {
        return leftColumn;
    }

    public QueryColumn getRightColumn() {
        return rightColumn;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public boolean insertRangeToJoinInfo(Range range){
        if(joinType.equals(JoinType.RIGHT) || joinType.equals(JoinType.LEFT)){
            if(leftColumn.getName().equals(range.getPath()) || rightColumn.getName().equals(range.getPath())){
                this.range = range;
                return true;
            }
        }
        return false;
    }

    public enum JoinType {
        INNER, LEFT, RIGHT, FULL;

        public static JoinType getType(Join join){
            if(join.isLeft())
                return LEFT;
            if(join.isRight())
                return RIGHT;
            if (join.isOuter() || join.isFull())
                return FULL;
            return INNER;
        }

        public static byte toCode(JoinType joinType) {
            if (joinType == null)
                return 0;
            switch (joinType) {
                case INNER: return 1;
                case LEFT: return 2;
                case RIGHT: return 3;
                case FULL: return 4;
                default: throw new IllegalArgumentException("Unsupported join type: " + joinType);
            }
        }

        public static JoinType fromCode(byte code) {
            switch (code) {
                case 0: return null;
                case 1: return INNER;
                case 2: return LEFT;
                case 3: return RIGHT;
                case 4: return FULL;
                default: throw new IllegalArgumentException("Unsupported join code: " + code);
            }
        }
    }

    public enum JoinAlgorithm {
        Nested, Hash, SortMerge
    }
}
