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
package org.apache.james.backends.es;

import java.util.Objects;

import org.elasticsearch.common.Strings;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

public record UpdatedRepresentation(DocumentId id, String updatedDocumentPart) {
    public UpdatedRepresentation {
        Preconditions.checkNotNull(id);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(updatedDocumentPart), "Updated document must be specified");
    }

    @Override
    public final boolean equals(Object o) {
        if (o instanceof UpdatedRepresentation other) {
            return Objects.equals(id, other.id)
                && Objects.equals(updatedDocumentPart, other.updatedDocumentPart);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("updatedDocumentPart", updatedDocumentPart)
            .toString();
    }
}