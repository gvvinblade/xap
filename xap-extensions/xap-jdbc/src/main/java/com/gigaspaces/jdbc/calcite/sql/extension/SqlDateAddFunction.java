package com.gigaspaces.jdbc.calcite.sql.extension;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * The <code>DATEADD</code> function, which adds an interval to a
 * datetime (TIMESTAMP, TIME or DATE).
 *
 * <p>The SQL syntax is
 *
 * <blockquote>
 * <code>DATEADD(<i>timestamp interval</i>, <i>quantity</i>,
 * <i>datetime</i>)</code>
 * </blockquote>
 *
 * <p>The interval time unit can one of the following literals:<ul>
 * <li>NANOSECOND (and synonym SQL_TSI_FRAC_SECOND)
 * <li>MICROSECOND (and synonyms SQL_TSI_MICROSECOND, FRAC_SECOND)
 * <li>SECOND (and synonym SQL_TSI_SECOND)
 * <li>MINUTE (and synonym  SQL_TSI_MINUTE)
 * <li>HOUR (and synonym  SQL_TSI_HOUR)
 * <li>DAY (and synonym SQL_TSI_DAY)
 * <li>WEEK (and synonym  SQL_TSI_WEEK)
 * <li>MONTH (and synonym SQL_TSI_MONTH)
 * <li>QUARTER (and synonym SQL_TSI_QUARTER)
 * <li>YEAR (and synonym  SQL_TSI_YEAR)
 * </ul>
 *
 * <p>Returns modified datetime.
 */
public class SqlDateAddFunction extends SqlFunction {

  private static final int MILLISECOND_PRECISION = 3;
  private static final int MICROSECOND_PRECISION = 6;

  private static final SqlReturnTypeInference RETURN_TYPE_INFERENCE =
      opBinding -> {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        return deduceType(typeFactory,
            opBinding.getOperandLiteralValue(0, TimeUnit.class),
            opBinding.getOperandType(1), opBinding.getOperandType(2));
      };

  public static RelDataType deduceType(RelDataTypeFactory typeFactory,
      TimeUnit timeUnit, RelDataType operandType1, RelDataType operandType2) {
    final RelDataType type;
    switch (timeUnit) {
    case HOUR:
    case MINUTE:
    case SECOND:
    case MILLISECOND:
    case MICROSECOND:
      switch (timeUnit) {
      case MILLISECOND:
        type = typeFactory.createSqlType(SqlTypeName.TIMESTAMP,
            MILLISECOND_PRECISION);
        break;
      case MICROSECOND:
        type = typeFactory.createSqlType(SqlTypeName.TIMESTAMP,
            MICROSECOND_PRECISION);
        break;
      default:
        if (operandType2.getSqlTypeName() == SqlTypeName.TIME) {
          type = typeFactory.createSqlType(SqlTypeName.TIME);
        } else {
          type = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        }
      }
      break;
    default:
      type = operandType2;
    }
    return typeFactory.createTypeWithNullability(type,
        operandType1.isNullable()
            || operandType2.isNullable());
  }

  /** Creates a SqlDateAddFunction. */
  SqlDateAddFunction() {
    super("DATEADD", SqlKind.OTHER_FUNCTION, RETURN_TYPE_INFERENCE, null,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER,
            SqlTypeFamily.DATETIME),
        SqlFunctionCategory.TIMEDATE);
  }
}