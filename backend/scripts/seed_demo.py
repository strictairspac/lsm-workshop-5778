from sqlalchemy import select
from src.config.database import Base,engine,Session
from src.models.user import User
from src.models.dataset import Dataset
from src.models.config import Config
from src.models.trial import Trial
from src.models.audit import Audit
from src.models.migration import Migration
from src.utils.password import hash_password
DEMO=[{'op':'put','key':'sensor-01','value':'18.2'},{'op':'put','key':'sensor-02','value':'19.1'},{'op':'put','key':'sensor-03','value':'22.4'},{'op':'put','key':'sensor-04','value':'17.0'},{'op':'put','key':'sensor-01','value':'18.8'},{'op':'delete','key':'sensor-02'},{'op':'put','key':'sensor-05','value':'21.6'},{'op':'put','key':'sensor-06','value':'20.3'},{'op':'delete','key':'sensor-03'},{'op':'put','key':'sensor-07','value':'23.0'},{'op':'put','key':'sensor-01','value':'19.0'},{'op':'put','key':'sensor-08','value':'18.9'}]
def seed():
    Base.metadata.create_all(engine)
    with Session() as db:
        for role in ('admin','operator','viewer'):
            email=role+'@beiling.test'
            if not db.scalar(select(User).where(User.email==email)):db.add(User(email=email,role=role,password_hash=hash_password('EdgeStore!17')))
        sets=[('北岭站点-早班写入','sensor-01重复写，sensor-02与sensor-03墓碑删除；12条操作，最终6个存活键。',DEMO),('旧值复活回归集','先将旧值刷入深层，再写墓碑；最终合并必须防止旧值复活。',[{'op':'put','key':'sensor-a','value':'old'},{'op':'put','key':'sensor-b','value':'b'},{'op':'put','key':'sensor-c','value':'c'},{'op':'put','key':'sensor-d','value':'d'},{'op':'delete','key':'sensor-a'},{'op':'put','key':'sensor-e','value':'e'}]),('全部删除边界集','两个已写入键随后全部删除，最终应是空字典。',[{'op':'put','key':'x','value':'1'},{'op':'put','key':'y','value':'2'},{'op':'delete','key':'x'},{'op':'delete','key':'y'}])]
        for name,desc,entries in sets:
            if not db.scalar(select(Dataset).where(Dataset.name==name)):db.add(Dataset(name=name,description=desc,entries=entries))
        configs=[('小内存-完整合并',2,2,3,True),('保留层级观测',2,3,3,False),('宽缓冲-完整合并',4,2,4,True)]
        for name,limit,trigger,depth,final in configs:
            if not db.scalar(select(Config).where(Config.name==name)):db.add(Config(name=name,memtable_limit=limit,l0_trigger=trigger,max_levels=depth,final_compaction=final))
        db.commit()
        seed_migrations(db)
        return {name:db.query(model).count() for name,model in [('users',User),('datasets',Dataset),('configs',Config),('trials',Trial),('migrations',Migration)]}


def seed_migrations(db):
    """迁移演练以自然键补齐；服务层负责建分片与版本，避免种子重复实现路由。"""
    from src.schemas.migration import MigrationInput
    from src.services.migration import create
    workload=('北岭站点-分片搬迁演练',[
        {'op':'put','key':'edge-k01','value':'11.0'},{'op':'put','key':'edge-k02','value':'11.2'},
        {'op':'put','key':'edge-k03','value':'11.4'},{'op':'put','key':'edge-k04','value':'11.6'},
        {'op':'put','key':'edge-k05','value':'11.8'},{'op':'put','key':'edge-k06','value':'12.0'},
        {'op':'put','key':'edge-k07','value':'12.2'},{'op':'put','key':'edge-k08','value':'12.4'},
        {'op':'delete','key':'edge-k06'}])
    name,entries=workload
    if not db.scalar(select(Dataset).where(Dataset.name==name)):
        db.add(Dataset(name=name,description='八个边缘键，range 搬迁 edge-k06..edge-k09，区间内含 k06 墓碑。',entries=entries))
        db.commit()
    dataset=db.scalar(select(Dataset).where(Dataset.name==name))
    if db.scalar(select(Migration).where(Migration.name=='早班-热点区间搬迁')):
        return 0
    create(db,MigrationInput(name='早班-热点区间搬迁',dataset_id=dataset.id,shard_count=2,range_start='edge-k06',range_end='edge-k09'))
    return 1
if __name__=='__main__':print(seed())
