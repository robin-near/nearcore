use std::{io, marker::PhantomData};

use borsh::{BorshDeserialize, BorshSerialize};

use crate::{DBCol, Store, StoreUpdate};

pub trait IsColumn {
    fn col(&self) -> DBCol;
}

pub struct ThinStore<Column: IsColumn> {
    store: Store,
    _phantom: PhantomData<fn() -> Column>,
}

impl<Column: IsColumn> Clone for ThinStore<Column> {
    fn clone(&self) -> Self {
        Self { store: self.store.clone(), _phantom: PhantomData }
    }
}

impl<Column: IsColumn> ThinStore<Column> {
    pub fn new(store: Store) -> Self {
        Self { store, _phantom: PhantomData }
    }

    pub fn get_ser<T: BorshDeserialize>(&self, col: Column, key: &[u8]) -> io::Result<Option<T>> {
        self.store.get_ser(col.col(), key)
    }

    pub fn store_update(&self) -> ThinStoreUpdate<Column> {
        ThinStoreUpdate { update: self.store.store_update(), _phantom: PhantomData }
    }
}

pub struct ThinStoreUpdate<Column: IsColumn> {
    update: StoreUpdate,
    _phantom: PhantomData<*const Column>,
}

impl<Column: IsColumn> ThinStoreUpdate<Column> {
    pub fn set_ser<T: BorshSerialize>(
        &mut self,
        col: Column,
        key: &[u8],
        value: &T,
    ) -> io::Result<()> {
        self.update.set_ser(col.col(), key, value)
    }

    pub fn insert_ser<T: BorshSerialize>(
        &mut self,
        col: Column,
        key: &[u8],
        value: &T,
    ) -> io::Result<()> {
        self.update.insert_ser(col.col(), key, value)
    }

    pub fn commit(self) -> io::Result<()> {
        self.update.commit()
    }
}

impl<Column: IsColumn> From<Store> for ThinStore<Column> {
    fn from(store: Store) -> Self {
        Self::new(store)
    }
}

impl<Column: IsColumn> From<ThinStoreUpdate<Column>> for StoreUpdate {
    fn from(update: ThinStoreUpdate<Column>) -> Self {
        update.update
    }
}
