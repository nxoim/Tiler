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

import com.tunjid.tiler.ListTiler
import com.tunjid.tiler.PivotRequest
import com.tunjid.tiler.QueryFetcher
import com.tunjid.tiler.Tile
import com.tunjid.tiler.TiledList
import com.tunjid.tiler.tiledListOf
import com.tunjid.tiler.toPivotedTileInputs
import com.tunjid.tiler.toTiledList
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.flow.update

interface GenericTiler<Query : Any, Item> {
    /**
     * State flow of all relevant tiles. Initial value is an empty list.
     */
    val relevantTiles: StateFlow<TiledList<Query, Item>>

    /**
     * Tiler will update all relevant and irrelevant data
     * in its's cache based on the most relevant query set.
     */
    fun setMostRelevantQuery(to: Query)
}

class GenericTilerConfiguration<T>(
    val activeCachedAmount: Int,
    val inactiveCachedAmount: Int,
    val itemSizeHint: Int? = null,
    val comparator: Comparator<T>
)

internal class GenericTilerImpl<Query : Any, Item>(
    private val coroutineScope: CoroutineScope,
    private val initial: Query,
    private val configuration: Flow<GenericTilerConfiguration<Query>>,
    private val fetcher: QueryFetcher<Query, Item>,
    private val listTilerBuilder: (
        order: Tile.Order<Query, Item>,
        limiter: Tile.Limiter<Query, Item>,
        fetcher: QueryFetcher<Query, Item>,
    ) -> ListTiler<Query, Item>,
    private val onNextQueryRequest: (sourceQuery: Query) -> Query?,
    private val onPreviousQueryRequest: (sourceQuery: Query) -> Query?,
) : GenericTiler<Query, Item> {
    private val mostRelevantQuery = MutableStateFlow<Query>(initial)

    override val relevantTiles = createGenericTiledListFlow(
        initial = initial,
        mostRelevantQuery = mostRelevantQuery,
        configuration = configuration,
        fetcher = fetcher,
        nextQuery = onNextQueryRequest,
        previousQuery = onPreviousQueryRequest,
        listTilerBuilder = listTilerBuilder
    ).stateIn(coroutineScope, SharingStarted.WhileSubscribed(), tiledListOf())

    override fun setMostRelevantQuery(to: Query) {
        mostRelevantQuery.update { to }
    }
}

/**
 * Combines all the requirements into one place and produces
 * a [TiledList] flow
 */
private fun <Query : Any, Item> createGenericTiledListFlow(
    initial: Query,
    mostRelevantQuery: StateFlow<Query>,
    configuration: Flow<GenericTilerConfiguration<Query>>,
    fetcher: QueryFetcher<Query, Item>,
    nextQuery: Query.() -> Query?,
    previousQuery: Query.() -> Query?,
    listTilerBuilder: (
        order: Tile.Order<Query, Item>,
        limiter: Tile.Limiter<Query, Item>,
        fetcher: QueryFetcher<Query, Item>,
    ) -> ListTiler<Query, Item>
): Flow<TiledList<Query, Item>> = configuration
    .map { configuration ->
        mostRelevantQuery
            .toPivotedTileInputs<Query, Item>(
                PivotRequest<Query, Item>(
                    onCount = configuration.activeCachedAmount,
                    offCount = configuration.inactiveCachedAmount,
                    comparator = configuration.comparator,
                    nextQuery = nextQuery,
                    previousQuery = previousQuery
                )
            )
            .toTiledList(
                listTiler = listTilerBuilder(
                    Tile.Order.PivotSorted(
                        query = initial,
                        comparator = configuration.comparator
                    ),
                    Tile.Limiter(
                        maxQueries = configuration.activeCachedAmount,
                        itemSizeHint = configuration.itemSizeHint
                    ),
                    fetcher
                )
            )
    }
    .flattenConcat()
