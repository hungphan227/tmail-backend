/****************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one   *
 * or more contributor license agreements.  See the NOTICE file *
 * distributed with this work for additional information        *
 * regarding copyright ownership.  The ASF licenses this file   *
 * to you under the Apache License, Version 2.0 (the            *
 * "License"); you may not use this file except in compliance   *
 * with the License.  You may obtain a copy of the License at   *
 *                                                              *
 *   http://www.apache.org/licenses/LICENSE-2.0                 *
 *                                                              *
 * Unless required by applicable law or agreed to in writing,   *
 * software distributed under the License is distributed on an  *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
 * KIND, either express or implied.  See the License for the    *
 * specific language governing permissions and limitations      *
 * under the License.                                           *
 ****************************************************************/

package org.apache.james.mailbox.elasticsearch.query;

import org.apache.james.backends.es.NodeMappingFactory;
import org.apache.james.mailbox.elasticsearch.json.JsonMessageConstants;
import org.apache.james.mailbox.model.SearchQuery;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;

public class SortConverter {

    private static final String PATH_SEPARATOR = ".";

    public static FieldSortBuilder convertSort(SearchQuery.Sort sort) {
        return getSortClause(sort.getSortClause())
            .order(getOrder(sort))
            .sortMode(SortMode.MIN);
    }

    private static FieldSortBuilder getSortClause(SearchQuery.Sort.SortClause clause) {
        return switch (clause) {
            case Arrival -> SortBuilders.fieldSort(JsonMessageConstants.DATE);
            case MailboxCc ->
                SortBuilders.fieldSort(JsonMessageConstants.CC + PATH_SEPARATOR + JsonMessageConstants.EMailer.ADDRESS
                    + PATH_SEPARATOR + NodeMappingFactory.RAW);
            case MailboxFrom ->
                SortBuilders.fieldSort(JsonMessageConstants.FROM + PATH_SEPARATOR + JsonMessageConstants.EMailer.ADDRESS
                    + PATH_SEPARATOR + NodeMappingFactory.RAW);
            case MailboxTo ->
                SortBuilders.fieldSort(JsonMessageConstants.TO + PATH_SEPARATOR + JsonMessageConstants.EMailer.ADDRESS
                    + PATH_SEPARATOR + NodeMappingFactory.RAW);
            case BaseSubject ->
                SortBuilders.fieldSort(JsonMessageConstants.SUBJECT + PATH_SEPARATOR + NodeMappingFactory.RAW);
            case Size -> SortBuilders.fieldSort(JsonMessageConstants.SIZE);
            case SentDate -> SortBuilders.fieldSort(JsonMessageConstants.SENT_DATE);
            case Uid -> SortBuilders.fieldSort(JsonMessageConstants.UID);
            case Id -> SortBuilders.fieldSort(JsonMessageConstants.MESSAGE_ID);
        };
    }

    private static SortOrder getOrder(SearchQuery.Sort sort) {
        if (sort.isReverse()) {
            return SortOrder.DESC;
        } else {
            return SortOrder.ASC;
        }
    }
}
