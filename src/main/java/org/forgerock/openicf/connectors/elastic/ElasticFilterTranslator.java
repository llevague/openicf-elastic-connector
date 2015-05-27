package org.forgerock.openicf.connectors.elastic;

import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.FilterBuilder;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.filter.*;

import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;

public class ElasticFilterTranslator extends AbstractFilterTranslator<FilterBuilder> {

    @Override
    protected FilterBuilder createAndExpression(FilterBuilder leftExpression, FilterBuilder rightExpression) {
        return andFilter(leftExpression, rightExpression);
    }

    @Override
    protected FilterBuilder createOrExpression(FilterBuilder leftExpression, FilterBuilder rightExpression) {
        return orFilter(leftExpression, rightExpression);
    }

    @Override
    protected FilterBuilder createContainsExpression(ContainsFilter filter, boolean not) {
        return wildcardFilter(filter, not, "*%s*");
    }

    @Override
    protected FilterBuilder createEndsWithExpression(EndsWithFilter filter, boolean not) {
        return wildcardFilter(filter, not, "*%s");
    }

    @Override
    protected FilterBuilder createEqualsExpression(EqualsFilter filter, boolean not) {
        final Attribute attribute = filter.getAttribute();
        final TermFilterBuilder termBuilder =
                termFilter(attribute.getName(), AttributeUtil.getSingleValue(attribute));

        return not ? boolFilter().mustNot(termBuilder) : boolFilter().must(termBuilder);
    }

    @Override
    protected FilterBuilder createGreaterThanExpression(GreaterThanFilter filter, boolean not) {
        return rangeFilter(filter, not, RangeEnum.GT);
    }

    @Override
    protected FilterBuilder createGreaterThanOrEqualExpression(GreaterThanOrEqualFilter filter, boolean not) {
        return rangeFilter(filter, not, RangeEnum.GTE);
    }

    @Override
    protected FilterBuilder createLessThanExpression(LessThanFilter filter, boolean not) {
        return rangeFilter(filter, not, RangeEnum.LT);
    }

    @Override
    protected FilterBuilder createLessThanOrEqualExpression(LessThanOrEqualFilter filter, boolean not) {
        return rangeFilter(filter, not, RangeEnum.LTE);
    }

    @Override
    protected FilterBuilder createStartsWithExpression(StartsWithFilter filter, boolean not) {
        return wildcardFilter(filter, not, "%s*");
    }

    @Override
    protected FilterBuilder createContainsAllValuesExpression(ContainsAllValuesFilter filter, boolean not) {
        final Attribute attribute = filter.getAttribute();
        final AndFilterBuilder andFilterBuilder = andFilter();
        for (Object value : attribute.getValue()) {
            andFilterBuilder.add(new TermFilterBuilder(attribute.getName(), value));
        }
        return andFilterBuilder;
    }

    private FilterBuilder wildcardFilter(StringFilter filter, boolean not, String searchPattern) {
        final QueryFilterBuilder queryFilterBuilder = queryFilter(
                wildcardQuery(filter.getName(), String.format(searchPattern, filter.getValue())));
        return not ? boolFilter().mustNot(queryFilterBuilder) : boolFilter().must(queryFilterBuilder);
    }

    private FilterBuilder rangeFilter(AttributeFilter filter, boolean not, RangeEnum compare) {
        final Attribute attribute = filter.getAttribute();
        final Object singleValue = AttributeUtil.getSingleValue(attribute);
        final RangeFilterBuilder rangeBuilder = new RangeFilterBuilder(attribute.getName());
        switch (compare) {
            case GT :
                rangeBuilder.gt(singleValue);
                break;
            case GTE :
                rangeBuilder.gte(singleValue);
                break;
            case LT :
                rangeBuilder.lt(singleValue);
                break;
            case LTE :
                rangeBuilder.lte(singleValue);
                break;
            default: throw new UnsupportedOperationException();
        }

        return not ? notFilter(rangeBuilder) : rangeBuilder;
    }

    private enum RangeEnum {
        GT, GTE, LT, LTE
    }
}
