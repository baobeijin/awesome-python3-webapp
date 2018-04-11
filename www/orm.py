#ORM全称“Object Relational Mapping”，即对象-关系映射，
#就是把关系数据库的一行映射为一个对象，也就是一个类对应一个表
# 与数据库操作有关，，SELECT INSERT UPDATE DELETE等封装起来
# 由于web APP框架使用了aiohttp异步模型，因此数据库也用异步模型。
# aiomysql为MYSQL数据库提偶供了一步IO的驱动 
import asyncio ,logging
import aiomysql
 
def log(sql,args=()):     #对logging.info封装，目的是方便输出sql语句 info是级别
      logging.info('SQL: %s' % sql )
      
# 创建连接池，每个HTTP请求都可以从连接池中直接获取数据库连接。
# 使用连接池的好处是不必频繁地打开和关闭数据库连接，而是能复用就尽量复用。
# 连接池由全局变量 _pool存储，
async def  create_pool(loop, **kw):
       logging.info('create database connection pool .....')
       global __pool
       __pool =await aiomysql.create_pool(
         host=kw.get('host','localhost'),
         port=kw.get('port',3306),
         user=kw['user'],
         password=kw['password'],
         db=kw['db'],
         charset=kw.get('charset','utf8'),        #必须设置否则从数据库得到的结果是乱码
         autocommit=kw.get('autocommit',True ),   # 增删改数据库时，TRUE代表不需要用commit来提交事务。
         maxsize=kw.get('maxsize',10),
         minsize=kw.get('minsize',1),
         loop=loop
       )

# select语句  需传入sql语句和参数，以及要查询数据的数量
async def select(sql ,args,sizze=None):
      log(sql,args)
      global __pool
      async with  __pool.get() as coon:                        #获取数据库连接     
            async with conn.cursor(aiomysql.DictCursor) as cur:#获取游标,默认游标返回的结果为元组,每一项是另一个元组,这里可以指定元组的元素为字典通过aiomysql.DictCursor
                  await cur.execute(sql.replace('?','%s'), args or ()) #调用游标的execute()方法来执行sql语句,execute()接收两个参数,
                                                                       #第一个为sql语句可以包含占位符,第二个为占位符对应的值,使用该形式可以避免直接使用字符串拼接出来的sql的注入攻击
                                                                       #SQL语句的占位符是?，而MySQL的占位符是%s，select()函数在内部自动替换
                  if size:                                             #size有值就获取对应数量的数据
                     rs=await cur.fetchmany(size)
                  else:                                                #获取所有数据库中的所有数据,此处返回的是一个数组,数组元素为字典
                     rs=await cur.fetchall()
            logging.info('rows returned: %s' % len(rs))
            return rs
             
 #封装增删改操作
async def execute(sql, args, qutocommit=True ):
       log(sql)
       async with __pool.get() as conn :
            if not autocommit:
                await conn.begin()
            try:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                      await  cur.execute(sql.replace('?','%s'), args)  
                      affected=cur.rowcount               # 获取增删改影响的行数
                if not autocommit:
                     await conn.commit()
            except BaseExpection as e:
                if not autocommit:
                     await conn.roolback()
                raise
            return affected                
#创建拥有几个占位符的字符串
def create_args_string(num):
    L=[]
    for n in rang(num):
         L.append('?')
    return ','.join(L)   

#保存数据库列名和类型的基类Field

class  Field(object):
    def _init_(self,name,column_type,primary_key,default):
        self.name=name                    #列名
        self.column_type=column_type      # 数据类型
        self.primary_key=primary_key      # 是否为主键
        self.default=default              #默认值
        
    def _str_(self):
        return '<%s,%s:%s>' %(self._class_._name_.self.column_type,self.name)
# Field的子类，，几种列名的数据类型
class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


#接下来编写类和元类
#元类
class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        if name=='Model':                                  #如果是基类不做处理
            return type.__new__(cls, name, bases, attrs)
        tableName = attrs.get('__table__', None) or name   #保存表名,如果获取不到,则把类名当做表名,完美利用了or短路原理
        logging.info('found model: %s (table: %s)' % (name, tableName))
        mappings = dict()                                  #保存列类型的对象
        fields = []                                        #保存保存列名的数组
        primaryKey = None                                  #主键
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键:
                    if primaryKey:
                        raise StandardError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise StandardError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
         #以下四种方法保存了默认了增删改查操作,其中添加的反引号``,是为了避免与sql关键字冲突的,否则sql语句会执行出错
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

#这是模型的基类,继承于dict,主要作用就是如果通过点语法来访问对象的属性获取不到的话,可以定制__getattr__来通过key来再次获取字典里的值
class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

#使用默认值
def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value    

#新的语法  @classmethod装饰器用于把类里面定义的方法声明为该类的类方法
@classmethod
#获取表里符合条件的所有数据,类方法的第一个参数为该类名
async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]
@classmethod
async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']       
@classmethod                      #主键查找的方法
async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])
#一下的都是对象方法,所以可以不用传任何参数,方法内部可以使用该对象的所有属性,及其方便
#保存实例到数据库 
async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)       

#更新数据库数据
async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)        

#删除数据
async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)                      
            
