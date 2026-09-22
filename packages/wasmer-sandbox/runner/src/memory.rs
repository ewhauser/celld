use std::ptr::NonNull;
use wasmer::{
    MemoryError, MemoryStyle, MemoryType, Pages, TableStyle, TableType,
    sys::{
        BaseTunables, NativeEngineExt, Tunables,
        vm::{VMMemory, VMMemoryDefinition, VMTable, VMTableDefinition},
    },
};

/// Limits committed guest memory separately from Wasmer's large virtual reservations.
struct Limits {
    base: BaseTunables,
    pages: Pages,
    elements: u32,
}

impl Limits {
    fn memory(&self, ty: &MemoryType) -> Result<MemoryType, MemoryError> {
        if ty.minimum > self.pages {
            return Err(MemoryError::Generic(
                "guest memory minimum exceeds quota".into(),
            ));
        }
        let mut bounded = *ty;
        bounded.maximum = Some(ty.maximum.unwrap_or(self.pages).min(self.pages));
        Ok(bounded)
    }

    fn table(&self, ty: &TableType) -> Result<TableType, String> {
        if ty.minimum > self.elements {
            return Err("guest table minimum exceeds quota".into());
        }
        let mut bounded = *ty;
        bounded.maximum = Some(ty.maximum.unwrap_or(self.elements).min(self.elements));
        Ok(bounded)
    }
}

impl Tunables for Limits {
    fn memory_style(&self, ty: &MemoryType) -> MemoryStyle {
        self.base.memory_style(ty)
    }

    fn table_style(&self, ty: &TableType) -> TableStyle {
        self.base.table_style(ty)
    }

    fn create_host_memory(
        &self,
        ty: &MemoryType,
        style: &MemoryStyle,
    ) -> Result<VMMemory, MemoryError> {
        self.base.create_host_memory(&self.memory(ty)?, style)
    }

    unsafe fn create_vm_memory(
        &self,
        ty: &MemoryType,
        style: &MemoryStyle,
        location: NonNull<VMMemoryDefinition>,
    ) -> Result<VMMemory, MemoryError> {
        // Wasmer owns the definition slot; the delegated allocator preserves its lifetime.
        unsafe {
            self.base
                .create_vm_memory(&self.memory(ty)?, style, location)
        }
    }

    fn create_host_table(&self, ty: &TableType, style: &TableStyle) -> Result<VMTable, String> {
        self.base.create_host_table(&self.table(ty)?, style)
    }

    unsafe fn create_vm_table(
        &self,
        ty: &TableType,
        style: &TableStyle,
        location: NonNull<VMTableDefinition>,
    ) -> Result<VMTable, String> {
        // Wasmer owns the definition slot, as in create_vm_memory.
        unsafe { self.base.create_vm_table(&self.table(ty)?, style, location) }
    }
}

fn bounded_store(pages: u32, elements: u32) -> wasmer::Store {
    let mut engine = wasmer::Engine::default();
    engine.set_tunables(Limits {
        base: BaseTunables::new(),
        pages: Pages(pages),
        elements,
    });
    wasmer::Store::new(engine)
}

pub fn store() -> wasmer::Store {
    bounded_store(8192, 1_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmer::{Instance, Module, imports};

    #[test]
    fn guest_growth_is_limited_even_with_large_declared_maximum() {
        let mut store = bounded_store(2, 2);
        let module = Module::new(
            &store,
            r#"(module
            (memory (export "memory") 1 65536)
            (table (export "table") 1 100 funcref))"#,
        )
        .unwrap();
        let instance = Instance::new(&mut store, &module, &imports! {}).unwrap();
        let memory = instance.exports.get_memory("memory").unwrap();
        assert_eq!(memory.grow(&mut store, 1).unwrap(), Pages(1));
        assert!(memory.grow(&mut store, 1).is_err());
        let table = instance.exports.get_table("table").unwrap();
        assert_eq!(
            table
                .grow(&mut store, 1, wasmer::Value::FuncRef(None))
                .unwrap(),
            1
        );
        assert!(
            table
                .grow(&mut store, 1, wasmer::Value::FuncRef(None))
                .is_err()
        );
    }

    #[test]
    fn oversized_minimum_is_rejected() {
        let mut store = bounded_store(2, 2);
        for wat in ["(module (memory 3))", "(module (table 3 funcref))"] {
            let module = Module::new(&store, wat).unwrap();
            assert!(Instance::new(&mut store, &module, &imports! {}).is_err());
        }
    }
}
