import{list}from'../api/migration.js';export const migrations={items:[],async load(){this.items=await list();return this.items}};
