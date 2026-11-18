import{datasets}from'../stores/dataset.js';import{migrations}from'../stores/migration.js';import{create,advance,resume,switchRoute,write,query,ownership,detail,download}from'../api/migration.js';import{header}from'../components/common/header.js';import{table}from'../components/common/table.js';import{esc}from'../utils/html.js';import{submit,notify}from'../hooks/feedback.js';import{session}from'../stores/session.js';

function rangeText(r){return (r.start_inclusive?'[':'(')+esc(r.start||'∅')+' , '+esc(r.end===null?'+∞':r.end)+(r.end_inclusive?']':')')}
function routeTable(routes){return table(['区间','归属分片','类型'],routes.map(r=>[rangeText(r),esc(r.shard_name||('#'+r.shard_id)),esc(r.shard_kind==='target'?'新分片（目标）':r.shard_kind==='source'?'旧分片（来源）':'—')]))}

export async function migrationsPage(el){
  const list=await migrations.load();const ds=await datasets.load();
  const [pathPart,queryPart]=location.hash.split('?');
  const ident=Number(pathPart.split('/')[1])||list[0]?.id;
  const writable=session.get().role!=='viewer';
  let current=null,own=null,snap=null;
  if(ident){try{current=await detail(ident);own=await ownership(ident)}catch{current=null}}
  const qs=new URLSearchParams(queryPart||'');
  const qStart=qs.get('start')||'',qEnd=qs.get('end')||'';
  if(current)snap=await query(ident,qStart||undefined,qEnd||undefined).catch(()=>null);

  const shardCards=current?current.shards.map(s=>`<span class="slab">${esc(s.name)} · ${esc(s.kind==='target'?'目标':'来源')}<br>${s.keys} keys / ${s.versions} versions<br>${esc(s.range_start===null?'—':(s.range_start||'∅'))} … ${esc(s.range_end===null?'+∞':s.range_end)}</span>`).join(''):'';

  el.innerHTML=header('05','把热点键区间搬到新分片','从批次建立可写分片，选择键区间迁往新分片。迁移期间仍可覆盖、删键和范围查询；切换把路由与目标数据放在同一个事务里，断点后从持久游标续迁。')+
  `<p class="modelnote">教学分片模型：版本、墓碑与写入水位保存在 MySQL；查询按发起时的已确认水位合并所有分片版本，重复副本只计最高序号。搬迁区间是闭区间 [起点, 终点]，路由用显式的开闭标记存储，因此端点键归入新分片，区间外的键仍有明确归属。</p>
  <div class="split"><section class="panel"><h2>迁移演练</h2>
  ${table(['演练','区间','状态','复制进度','归属'],list.map(x=>[`<a href="#migrations/${x.id}">${esc(x.name)}</a><br><small>#${x.id} · 水位 ${x.watermark}</small>`,`${esc(x.range_start)} … ${esc(x.range_end)}`,`<span class="badge ${esc(x.stage)}">${esc(x.stage_label)}</span>`,`${x.copy_batches} 批 / ${x.copied_rows} 版本`,x.status==='switched'?'<span class="badge passed">新分片</span>':'<span class="badge">来源分片</span>']))}
  ${current?`<div class="actions" id="migration-actions" style="margin-top:24px">
    <button id="advance" ${writable?'':'disabled'}>推进一个复制批次 →</button>
    <button id="resume" ${writable?'':'disabled'}>续迁至复制完成</button>
    <button id="switch" class="primary" ${writable?'':'disabled'}>切换路由与目标</button>
    <button id="export">下载迁移 JSON ↓</button>
  </div><p class="helper">当前状态 ${esc(current.stage_label)}（路由 ${esc(current.status_label)}）。推进只复制一个批次；续迁从数据库游标继续，已复制版本不重复写入。切换要求复制完成，且与并发写入共用行锁。</p>`:''}
  ${current?`<h2 style="margin-top:34px">分片与路由</h2><div class="layers"><div class="layer"><span class="mono">${esc(current.status==='switched'?'NEW':'OLD')}</span><div class="slabs">${shardCards}</div></div></div>
  <h3 style="margin-top:24px">当前路由归属</h3>${routeTable(current.routes)}
  ${own?`<p class="helper">迁移区间 ${esc(own.range.start)} … ${esc(own.range.end)} 当前归属 ${esc(own.range.owner_shard_name)}（${own.range.owner_shard_kind==='target'?'已切换':'来源'}）；目标分片已暂存 ${own.target_staged_versions} 个版本。</p>${table(['分片','角色','区间','版本','归属状态'],own.shards.map(s=>[esc(s.name),esc(s.kind==='target'?'目标':'来源'),(s.range_start===null?'—':esc(s.range_start||'∅'))+' … '+esc(s.range_end===null?'+∞':s.range_end),s.versions,`<span class="badge ${s.ownership==='已切换归属'?'passed':''}">${esc(s.ownership)}</span>`]))}`:''}
  <h3 style="margin-top:24px">迁移轨迹</h3>${table(['步骤','事件','细节'],current.history.slice(-12).reverse().map(h=>[h.step,esc(h.event),esc(Object.entries(h).filter(([k])=>!['step','event','status'].includes(k)).map(([k,v])=>k+'='+JSON.stringify(v)).join(' '))]))}`:'<div class="empty">暂无迁移演练。先登记写入批次，再建立分片与搬迁区间。</div>'}
  </section><aside class="panel"><h2>${writable?'建立迁移演练':'只读演练'}</h2>
  ${writable?`<form id="migration-form"><label>演练名称</label><input name="name" required maxlength="120" value="热点区间-${Date.now().toString().slice(-6)}"><label>键值写入批次</label><select name="dataset_id">${ds.map(x=>`<option value="${x.id}">${esc(x.name)} / ${x.distinct_keys}键</option>`).join('')}</select><label>初始分片数 · 2–4</label><input type="number" name="shard_count" min="2" max="4" value="${current?.shard_count||3}" required><label>搬迁区间起点（含）</label><input name="range_start" maxlength="64" required value="${esc(current?.range_start||'')}" placeholder="edge-k06"><label>搬迁区间终点（含）</label><input name="range_end" maxlength="64" required value="${esc(current?.range_end||'')}" placeholder="edge-k09"><div class="actions"><button class="primary" type="submit">建立可写分片 →</button></div></form><p class="helper">区间必须完整落在一个来源分片内。建立后立即写入口：覆盖、删键都会生成新的版本与水位。</p>`:'<p class="helper">观察员可查看路由、查询与导出；写入、推进与切换在 API 层返回 403。</p>'}
  ${current?`<h2 style="margin-top:34px">覆盖 / 删键</h2><form id="write-form"><label>键</label><input name="key" maxlength="64" required value="${esc(current.range_start)}"><label>操作</label><select name="op"><option value="put">put 覆盖</option><option value="delete">delete 墓碑</option></select><label>值（delete 忽略）</label><input name="value" maxlength="200" value="18.6"><div class="actions"><button type="submit" ${writable?'':'disabled'}>写入分片</button></div></form>
  <h2 style="margin-top:34px">范围查询</h2><form id="query-form"><label>起点</label><input name="start" maxlength="64" value="${esc(qStart||current.range_start)}"><label>终点</label><input name="end" maxlength="64" value="${esc(qEnd||current.range_end)}"><div class="actions"><button type="submit">执行范围查询</button></div></form>`:''}
  </aside></div>
  ${snap?`<section class="panel"><div class="actions" style="margin-top:0"><h2 style="margin:0">查询快照 · 水位 ${snap.watermark}</h2>${snap.verified?'<span class="badge passed">与写入 Oracle 一致</span>':''}</div>
  <div class="metrics"><div><small>返回键数</small><strong>${snap.count}</strong></div><div><small>扫描版本</small><strong>${snap.scanned_versions}</strong></div><div><small>合并重复副本</small><strong>${snap.duplicates_merged}</strong></div><div><small>墓碑生效</small><strong>${snap.tombstones_applied}</strong></div></div>
  <p class="helper">发起时已确认水位 ${snap.watermark}；读取分片 ${esc((snap.shards_read_names||[]).join('、')||'—')}；按键升序、不重不漏。checksum ${esc(snap.checksum.slice(0,24))}…</p>
  ${table(['键','值','写入序号','版本所在分片','归属分片','副本数'],snap.rows.map(r=>[esc(r.key),esc(r.value),r.seq,esc(r.version_shard_name)+' / '+esc(r.version_shard_kind),esc(r.owner_shard_name),r.replicated_copies]))}</section>`:''}`;

  if(writable&&current){
    const actions=el.querySelector('#migration-actions');
    const act=(buttonId,fn)=>actions?.querySelector('#'+buttonId).addEventListener('click',()=>submit(actions,async()=>{const r=await fn(ident);notify(r.status==='switched'?'路由已切换，目标分片接管':'迁移已推进');await migrationsPage(el)}));
    act('advance',advance);act('resume',resume);act('switch',switchRoute);
  }
  el.querySelector('#export')?.addEventListener('click',()=>download(ident).catch(e=>notify(e.message)));
  el.querySelector('#migration-form')?.addEventListener('submit',e=>{e.preventDefault();submit(e.target,async()=>{const d=Object.fromEntries(new FormData(e.target));d.dataset_id=Number(d.dataset_id);d.shard_count=Number(d.shard_count);const m=await create(d);notify('迁移演练已建立，分片可写');location.hash='migrations/'+m.id;await migrationsPage(el)})});
  el.querySelector('#write-form')?.addEventListener('submit',e=>{e.preventDefault();submit(e.target,async()=>{const d=Object.fromEntries(new FormData(e.target));if(d.op==='delete')delete d.value;const r=await write(ident,d);notify(`已写入 ${r.shard_name} · 水位 ${r.seq} · ${r.phase==='after_switch'?'切换后':'切换前'}`);await migrationsPage(el)})});
  el.querySelector('#query-form')?.addEventListener('submit',e=>{e.preventDefault();const d=Object.fromEntries(new FormData(e.target));location.hash='migrations/'+ident+'?start='+encodeURIComponent(d.start)+'&end='+encodeURIComponent(d.end)});
}
