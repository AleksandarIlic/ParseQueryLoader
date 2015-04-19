/*
 * Copyright (C) 2015 Aleksandar Ilic
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package com.aleksandarilic.parse;

import android.content.Context;
import android.support.annotation.NonNull;
import android.support.v4.content.AsyncTaskLoader;
import android.util.Log;

import com.parse.ParseException;
import com.parse.ParseObject;
import com.parse.ParseQuery;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * A loader that loads {@link com.parse.ParseObject}s from a {@link ParseQuery}
 * supplied to this loader and returns the result as a {@link java.util.List}.
 * <p/>
 * This class implements the {@link android.content.Loader} protocol in a standard way,
 * building on {@link android.content.AsyncTaskLoader} to perform the ParseQuery on a
 * background thread so that it does not block the application's UI.
 */
public class ParseQueryLoader<T extends ParseObject> extends AsyncTaskLoader<List<T>> {

    /**
     * Debug tag.
     */
    private static final String TAG = ParseQueryLoader.class.getSimpleName();

    /**
     * Max results per query limited by Parse.
     */
    private static final int QUERY_LIMIT = 1000;

    /**
     * Default value for objects per page.
     */
    private static final int DEFAULT_OBJECTS_PER_PAGE = 25;

    /**
     * Loaded objects by this loader.
     */
    private List<T> mObjects;

    /**
     * Underlying query for loading objects.
     */
    private ParseQuery<T> mParseQuery;

    /**
     * Boolean indicating if this loader should load all objects returned by query.
     */
    private boolean mForceLoadEverything;

    /**
     * Number of objects per page/loading.
     */
    private int mObjectsPerPage;

    /**
     * Current page of the underlying query.
     */
    private int mCurrentPage;

    /**
     * Next page to load from underlying query.
     */
    private int mPageToLoad;

    /**
     * Boolean indicating if there are more pages to load.
     */
    private boolean mHasNextPage;

    /**
     * Creates an empty unspecified ParseQueryLoader. You must follow this with
     * calls to {@link #setParseQuery(com.parse.ParseQuery)} to specify the query.
     *
     * @param context Context of the loader.
     * @see #setParseQuery(com.parse.ParseQuery)
     * @see #setObjectsPerPage(int)
     *
     */
    public ParseQueryLoader(Context context) {
        this(context, null);
    }

    /**
     * Creates a customized ParseQueryLoader to load {@link ParseObject}s from
     * a given {@link com.parse.ParseQuery}.
     *
     * @param context    Context of the loader
     * @param parseQuery ParseQuery to use when loading the data.
     */
    public ParseQueryLoader(Context context, ParseQuery<T> parseQuery) {
        super(context);
        mParseQuery = parseQuery;
        mObjectsPerPage = DEFAULT_OBJECTS_PER_PAGE;
    }

    /**
     * Sets the underlying {@link com.parse.ParseQuery} for loading the data.
     *
     * @param parseQuery ParseQuery to use when loading the data.
     * @return this, for chaining.
     */
    public ParseQueryLoader<T> setParseQuery(ParseQuery<T> parseQuery) {
        mParseQuery = parseQuery;
        return this;
    }

    /**
     * Sets the object count per page to load.
     *
     * @param objectsPerPage number of objects to load per page
     * @return this, for chaining.
     */
    public ParseQueryLoader<T> setObjectsPerPage(int objectsPerPage) {
        mObjectsPerPage = objectsPerPage;
        return this;
    }

    /**
     * Sets the flag to force the loader to load all objects from underlying
     * {@link com.parse.ParseQuery}.
     *
     * @param loadEverything <code>true</code> to load all objects,
     *                       otherwise pagination will be enabled
     * @return this, for chaining.
     */
    public ParseQueryLoader setForceLoadEverything(boolean loadEverything) {
        mForceLoadEverything = loadEverything;
        return this;
    }

    /**
     * This is where the bulk of our work is done. This function is
     * called in a background thread and should generate a new set of
     * data to be published by the loader.
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<T> loadInBackground() {
        if (mParseQuery == null) {
            return Collections.emptyList();
        }

        try {
            return mForceLoadEverything ? findAll(mParseQuery) : loadObjects(mPageToLoad);
        } catch (ParseException e) {
            if (BuildConfig.DEBUG) {
                Log.w(TAG, "Load interrupted with ParseException " + e.getCode()
                        + ": " + e.getMessage());
            }

            return Collections.emptyList();
        }
    }

    /**
     * Finds all {@link ParseObject}s from given {@link ParseQuery}.
     *
     * @param parseQuery parse query to use for querying objects
     * @return {@link List} with {@link ParseObject}s returned by given {@link ParseQuery}.
     *         Empty list in case of no results, never {@code null}.
     * @throws ParseException
     */
    @NonNull
    private List<T> findAll(ParseQuery<T> parseQuery) throws ParseException {
        int skip = 0;
        List<T> allObjects = new LinkedList<T>();
        parseQuery.setSkip(skip).setLimit(QUERY_LIMIT);

        boolean hasMore = true;
        while (hasMore) {
            List<T> foundObjects = parseQuery.find();
            allObjects.addAll(foundObjects);

            // Incrementing skip
            skip += QUERY_LIMIT;
            parseQuery.setSkip(skip);

            // If we have loaded as many objects as limit,
            // there might be more objects to load
            hasMore = foundObjects.size() == QUERY_LIMIT;
        }

        return allObjects;
    }


    @NonNull
    private List<T> loadObjects(int pageIndex) throws ParseException {
        if (mObjectsPerPage > 0) {
            // Adding +1 object to find out if there is next page
            mParseQuery.setLimit(mObjectsPerPage + 1);
            mParseQuery.setSkip(pageIndex * mObjectsPerPage);
        }

        List<T> foundObjects = mParseQuery.find();

        if (pageIndex >= mCurrentPage) {
            mCurrentPage = pageIndex;
            mHasNextPage = foundObjects.size() > mObjectsPerPage;
        }

        // Removing +1 object from the end which we used to find out if has next page
        if (mHasNextPage) {
            foundObjects.remove(mObjectsPerPage);
        }

        return foundObjects;
    }

    /**
     * Starts loading the next page from the underlying {@link com.parse.ParseQuery}.
     * Make sure that there is available next page to load, otherwise
     * {@link java.lang.RuntimeException} will be thrown.
     * <p/>
     * <p>Must be called from the process's main thread.
     *
     * @see #hasNextPage()
     */
    public void loadNextPage() {
        if (!hasNextPage()) {
            throw new RuntimeException("There are no more pages to load. Consider " +
                    "checking if hasNextPage() before calling loadNextPage().");
        }

        mPageToLoad = mCurrentPage + 1;
        onContentChanged();
    }

    /**
     * Checks if the underlying {@link com.parse.ParseQuery} has next page of unloaded data.
     *
     * @return <code>true</code> if there are more data to be loaded, otherwise <code>false</code>
     * @see #loadNextPage()
     */
    public boolean hasNextPage() {
        return mObjects != null && !mObjects.isEmpty() && mHasNextPage;
    }

    /**
     * Called when there is new data to deliver to the client.  The
     * super class will take care of delivering it; the implementation
     * here just adds a little more logic.
     */
    @Override
    public void deliverResult(List<T> objects) {
        if (isReset()) {
            // An async query came in while the loader is stopped.
            // We don't need the result.
            return;
        }

        // Initialize the list for keeping loaded objects
        List<T> oldObjects = mObjects;
        mObjects = new LinkedList<T>();
        if (oldObjects != null) {
            mObjects.addAll(oldObjects);
        }
        mObjects.addAll(objects);

        if (isStarted()) {
            // If the Loader is currently started, we can immediately
            // deliver its results.
            super.deliverResult(mObjects);
        }
    }

    /**
     * Handles a request to start the Loader.
     */
    @Override
    protected void onStartLoading() {
        if (mObjects != null) {
            // If we currently have a result available, deliver it immediately.
            deliverResult(mObjects);
        }

        // Listen for data changes
        onRegisterParseQueryObserver();

        if (takeContentChanged() || mObjects == null) {
            // If the data has changed since the last time it was loaded
            // or is not currently available, start a load.
            forceLoad();
        }
    }

    /**
     * Handles a request to stop the Loader.
     */
    @Override
    protected void onStopLoading() {
        // Attempt to cancel the current load task if possible.
        cancelLoad();
    }

    /**
     * Handles a request to completely reset the Loader.
     */
    @Override
    protected void onReset() {
        super.onReset();

        // Ensure the loader is stopped
        onStopLoading();

        // At this point we can release the loaded ParseObjects
        if (mObjects != null) {
            mObjects = null;
        }

        // Stop listening for data changes
        onUnregisterParseQueryObserver();
    }

    /**
     * Callback for registering custom observer for data by underlying
     * {@link com.parse.ParseQuery}. Implementations should provide it's
     * own mechanism of observing the data changes.
     * <p/>
     * <p>Must be called from the process's main thread.
     *
     * @see #onUnregisterParseQueryObserver()
     */
    protected void onRegisterParseQueryObserver() {

    }

    /**
     * Callback for unregistering custom observer for data by underlying
     * {@link com.parse.ParseQuery}. Implementations should unregister
     * here any observer registered in {@link #onRegisterParseQueryObserver}.
     * <p/>
     * <p>Must be called from the process's main thread.
     *
     * @see #onRegisterParseQueryObserver()
     */
    protected void onUnregisterParseQueryObserver() {

    }

    /**
     * Notifies the loader that data returned by underlying {@link com.parse.ParseQuery}
     * changed and should be reloaded to reflect the latest changes.
     * <p/>
     * <p>Must be called from the process's main thread.
     */
    protected void notifyDataChanged() {
        onContentChanged();
    }

}