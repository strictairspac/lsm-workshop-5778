/** @typedef {{id:number,name:string,description:string,entries:Array}} Dataset */
/** @typedef {{id:number,name:string,memtable_limit:number,l0_trigger:number,max_levels:number,final_compaction:boolean}} Config */
/** @typedef {{id:number,name:string,dataset_id:number,config_id:number,status:string,result:Object}} Trial */
/** @typedef {{id:number,name:string,status:'pending'|'copying'|'switched',range_start:string,range_end:string,watermark:number,routes:Array,shards:Array}} Migration */
export const entityNames={dataset:'键值批次',config:'存储配置',trial:'合并试验',migration:'迁移演练'};
