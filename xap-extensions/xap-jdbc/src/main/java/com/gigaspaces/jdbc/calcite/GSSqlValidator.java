package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope.ResolvedImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil.Suggester;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class GSSqlValidator extends SqlValidatorImpl {
    private static final String GEN_PREFIX = "$$_";
    private static final String GEN_SUFFIX = "_$$";
    private static final Pattern GEN_PATTERN = Pattern.compile("\\$\\$_\\d+_(.+)_\\$\\$");

    private static final Suggester GENERATED =
        (orig, attempt, size) -> GEN_PREFIX + Math.max(attempt, size) + "_" + Util.first(orig, "EXP$") + GEN_SUFFIX;

    private final Set<SqlNode> renamedNodes = Collections.newSetFromMap(new IdentityHashMap<>());

    /**
     * Creates a validator.
     *
     * @param opTab         Operator table
     * @param catalogReader Catalog reader
     * @param typeFactory   Type factory
     * @param config        Config
     */
    protected GSSqlValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory, Config config) {
        super(opTab, catalogReader, typeFactory, config);
    }

    @Override
    protected void addToSelectList(List<SqlNode> list, Set<String> aliases, List<Map.Entry<String, RelDataType>> fieldList, SqlNode exp, SelectScope scope, boolean includeSystemVars) {
        String alias = SqlValidatorUtil.getAlias(exp, -1);
        String uniqueAlias =
            SqlValidatorUtil.uniquify(
                alias, aliases, GENERATED);
        if (!Objects.equals(alias, uniqueAlias)) {
          exp = SqlValidatorUtil.addAlias(exp, uniqueAlias);
          renamedNodes.add(exp);
        }
        fieldList.add(Pair.of(uniqueAlias, deriveType(scope, exp)));
        list.add(exp);
    }

    public List<String> getOriginalNames(SqlNode validated) {
        if (!renamedNodes.isEmpty() && validated.isA(SqlKind.TOP_LEVEL)) {
            switch (validated.getKind()) {
                case SELECT: {
                    SqlValidatorNamespace namespace = getNamespace(validated);
                    return getValidatedNodeType(validated).getFieldNames().stream()
                        .map(field -> lookupName(field, namespace))
                        .collect(Collectors.toList());
                }
                case UNION:
                case INTERSECT:
                case EXCEPT: {
                    SqlCall call = (SqlCall) validated;
                    return getOriginalNames(call.operand(0));
                }
                case WITH: {
                    SqlWith with = (SqlWith) validated;
                    return getOriginalNames(with.body);
                }
            }
        }

        return getValidatedNodeType(validated).getFieldNames();
    }

    private String lookupName(String origin, SqlValidatorNamespace namespace) {
        String restored = lookupName0(origin, namespace);
        if (restored.startsWith(GEN_PREFIX)) { // fallback
            Matcher matcher = GEN_PATTERN.matcher(restored);
            if (matcher.matches()) {
                return matcher.group(1);
            }
        }
        return restored;
    }

    private String lookupName0(String origin, SqlValidatorNamespace namespace) {
        if (namespace == null)
            return origin;

        SqlNode node = namespace.getNode();
        if (node == null)
            return origin;

        switch (node.getKind()) {
            case SELECT:
                RelDataTypeField field = getValidatedNodeType(node)
                    .getField(origin, true, false);
                SqlNode col = Objects.requireNonNull(
                    ((SqlSelect) node).getSelectList().get(field.getIndex()));

                if (col.getKind() == SqlKind.AS) {
                    if (!renamedNodes.contains(col))
                        return SqlValidatorUtil.getAlias(col, -1);

                    col = ((SqlCall) col).getOperandList().get(0);
                    origin = SqlValidatorUtil.getAlias(col, -1);
                }

                if (col instanceof SqlIdentifier) {
                    SqlIdentifier identifier = (SqlIdentifier) col;
                    if (identifier.names.size() > 1) {
                        SqlIdentifier prefix = identifier.skipLast(1);
                        ResolvedImpl resolved = new ResolvedImpl();
                        SqlNameMatcher nameMatcher = getCatalogReader().nameMatcher();
                        getSelectScope((SqlSelect) node).resolve(prefix.names, nameMatcher, true, resolved);
                        return resolved.count() == 1
                            ? lookupName0(origin, resolved.only().namespace.resolve())
                            : SqlValidatorUtil.getAlias(identifier, -1);
                    }
                }

                return origin;
            case UNION:
            case EXCEPT:
            case INTERSECT:
                return lookupName0(origin, getNamespace(((SqlCall)node).operand(0)));

            default:
                return origin;
        }
    }
}
