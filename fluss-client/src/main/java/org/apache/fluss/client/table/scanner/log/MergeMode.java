/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Controls how the {@link MergedLogScanner} handles misalignment between parent and overlay
 * scanners.
 *
 * @since 0.6
 */
@PublicEvolving
public enum MergeMode {

    /** Block until both parent and overlay records are available for a given offset. */
    WAIT,

    /** Emit parent rows with NULL enrichment columns if overlay has not caught up. */
    BEST_EFFORT
}
