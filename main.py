from pymongo import MongoClient
import gridfs
import io


class Test:
    def __init__(self, username, password, sourcefile, replica, filedb):
        self.username = username
        self.password = password
        self.sourcefile = sourcefile
        self.replica = replica
        self.filedb = filedb

    def createGridFS(self):
        client = MongoClient('mongodb://%s:%s@'
                             '192.168.81.141:27017,'
                             '192.168.81.142:27017,'
                             '192.168.81.143:27017'
                             '/?authSource=%s&replicaSet=%s' %
                             (self.username, self.password, self.sourcefile, self.replica));
        db = client[self.filedb]
        fs = gridfs.GridFS(db)
        return fs

    def insertGridFS(self, file_path, file_name, fs):
        if fs.exists(file_name):
            print("文件已经存在！！！")
        else:
            with open(file_path, 'rb') as fileObj:
                data = fileObj.read()
                ObjectId = fs.put(data, filename=file_path.split('/')[-1])
                print(ObjectId)
                fileObj.close()
            return ObjectId

    def getFileProperty(self, fs, id):
        gf = fs.get(id)  # 通过文件id获取文件属性对象
        dbdata = gf.read()  # 二进制数据
        attri = {}  # 文件属性信息
        attri['chunk_size'] = gf.chunk_size  # 块大小
        attri['length'] = gf.length  # 文件大小
        attri["upload_date"] = gf.upload_date  # 上传日期
        attri["filename"] = gf.filename  # 文件名
        attri['md5'] = gf.md5  # md5
        print(attri)  # 打印文件属性信息
        return (dbdata, attri)  # 返回文件属性信息和文件二进制数据

    def getFiles(self, file_name, fs):
        ObjectId = fs.find_one(file_name)._id
        print('文件列表:%s' % (fs.list()))
        print('%s文件id:%s' % (file_name.get('filename'), ObjectId))
        return ObjectId

    def downloadFile(self, dbdata, download_path):
        output = io.open(download_path, mode='wb')
        output.write(dbdata)
        output.close()
        print('download ok!')

    def deleteFile(self, id, fs):
        fs.delete(id)
        print('delete ok!')


if __name__ == '__main__':
    # 实例化类Test并传入连接参数，创建GridFS连接
    test = Test("itcastAdmin", "123456", "admin", "itcast", "testfiles")
    fs = test.createGridFS()
    # 上传文件，返回文件id
    print('*********上传文件*****************')
    file_name = {'filename': 'testdata.csv'}
    file_path = './data/testdata.csv'
    id = test.insertGridFS(file_path, file_name, fs) \
    # 通过文件名获取文件id
    print('*********获取文件id并打印文件列表***************')
    ObjectId = test.getFiles({'filename': 'testdata.csv'}, fs)
    # 获取文件属性attri和文件二进制数据bdata
    print('*********获取文件元数据信息及二进制数据**************')
    (dbdata, attri) = test.getFileProperty(fs, ObjectId)
    # 下载文件到本地
    print('**********下载文件**************')
    download_path = './data/downloadFile/%s' % (attri['filename'])
    test.downloadFile(dbdata, download_path)
    # 删除文件
    print('***********删除文件**************')
    test.deleteFile(ObjectId, fs)
