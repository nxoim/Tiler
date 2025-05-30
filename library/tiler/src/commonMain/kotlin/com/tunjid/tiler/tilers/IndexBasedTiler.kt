/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tunjid.tiler.tilers

import com.tunjid.tiler.QueryFetcher
import com.tunjid.tiler.concurrentListTiler
import com.tunjid.tiler.utilities.neighboredQueryFetcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf

/**
 * Covers tiling of data that can be fetched by their index from a source.
 *
 * Example:
 * ```kotlin
 * class YourModel {
 *     private val tiler = IndexBasedTiler<YourItemType>(
 *         coroutineScope = coroutineScope,
 *         initialIndex = 0,
 *         activeCachedAmount = 3 // at least 3,
 *         inactiveCachedAmount = 3,
 *         fetcher = { index ->
 *             // this is a page of your items, Flow<List<YourItemType>>
 *             yourSource.getPotentiallyUpdatablePageFlow(index)
 *         }
 *     )
 *
 *     val content = tiler.relevantTiles
 *
 *     // this will be called by your ui while scrolling
 *     fun setMostRelevantPageIndex(index: Long) {
 *         tiler.setMostRelevantQuery(index)
 *     }
 * }
 * ```
 *@param initialIndex The index tiling will start from
 *
 * @param activeCachedAmount The amount of concurrent queries to collect from at any one time. Must be at least 3
 *
 * @param inactiveCachedAmount The amount of queries to keep in memory, but not collect from
 *
 * @param itemSizeHint Optimizes retrieval speed for items fetched.
 * Use only if you can guarantee that your queries return a fixed number
 * of items each time, for example SQL queries with a limit parameter.
 *
 * @see neighboredQueryFetcher
 */
@Suppress("FunctionName")
fun <T> IndexBasedTiler(
    coroutineScope: CoroutineScope,
    initialIndex: Long,
    activeCachedAmount: Int,
    inactiveCachedAmount: Int,
    itemSizeHint: Int? = null,
    fetcher: QueryFetcher<Long, T>,
): GenericTiler<Long, T>  = IndexBasedTiler(
    coroutineScope,
    initialIndex,
    flowOf(
        IndexBasedTilerConfiguration(
            activeCachedAmount = activeCachedAmount,
            inactiveCachedAmount = inactiveCachedAmount,
            itemSizeHint = itemSizeHint
        )
    ),
    fetcher
)

/**
 * Covers tiling of data that can be fetched by their index from a source.
 * This overload accepts a flow of configurations for adapting to ui.
 *
 * Example:
 * ```kotlin
 * class YourModel {
 *     private val gridColumns = MutableStateFlow(1)
 *     private val tiler = IndexBasedTiler<YourItemType>(
 *         coroutineScope = coroutineScope,
 *         initialIndex = 0,
 *         configuration = gridColumns.map {
 *             IndexBasedTilerConfiguration(
 *                 activeCachedAmount = (3 * it).coerceAtLeast(3), // at least 3
 *                 inactiveCachedAmount = 1 * it
 *             )
 *         },
 *         fetcher = { index ->
 *             // this is a page of your items, Flow<List<YourItemType>>
 *             yourSource.getPotentiallyUpdatablePageFlow(index)
 *         }
 *     )
 *
 *     val content = tiler.relevantTiles
 *
 *     // this will be called by your ui while scrolling
 *     fun setMostRelevantPageIndex(to: Long) {
 *         tiler.setMostRelevantQuery(to)
 *     }
 *
 *     // this and other similar functions will be useful
 *     // for changing the configuration of tiling
 *     fun setVisibleGridColumns(to: Int) {
 *         gridColumns.update { to }
 *     }
 * }
 * ```
 *@param initialIndex The index tiling will start from
 *
 * @see neighboredQueryFetcher
 */
@Suppress("FunctionName")
fun <Item> IndexBasedTiler(
    coroutineScope: CoroutineScope,
    initialIndex: Long,
    configuration: Flow<GenericTilerConfiguration<Long>>,
    fetcher: QueryFetcher<Long, Item>,
): GenericTiler<Long, Item> = GenericTilerImpl<Long, Item>(
    coroutineScope = coroutineScope,
    initial = initialIndex,
    configuration = configuration,
    fetcher = fetcher,
    listTilerBuilder = ::concurrentListTiler,
    onNextQueryRequest = { sourceQuery -> sourceQuery + 1 },
    onPreviousQueryRequest = { sourceQuery ->  (sourceQuery - 1).takeIf { it >= 0 } },
)

/**
 * @param activeCachedAmount The amount of concurrent queries to collect from at any one time. Must be at least 3
 *
 * @param inactiveCachedAmount The amount of queries to keep in memory, but not collect from
 *
 * @param itemSizeHint Optimizes retrieval speed for items fetched.
 * Use only if you can guarantee that your queries return a fixed number
 * of items each time, for example SQL queries with a limit parameter.
 */
@Suppress("FunctionName")
fun IndexBasedTilerConfiguration(
    activeCachedAmount: Int,
    inactiveCachedAmount: Int,
    itemSizeHint: Int? = null
) = GenericTilerConfiguration(
    activeCachedAmount = activeCachedAmount,
    inactiveCachedAmount = inactiveCachedAmount,
    comparator = Comparator<Long>(Long::compareTo),
    itemSizeHint = itemSizeHint
)

