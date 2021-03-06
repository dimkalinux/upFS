#!/usr/bin/python -O
# -*- coding: utf-8 -*-

import os, stat, errno, sys, random, MySQLdb, hashlib
from time import time
from subprocess import *
from tempfile import NamedTemporaryFile, mkstemp

try:
    import _find_fuse_parts
except ImportError:
    pass

import fuse
from fuse import Fuse


if not hasattr(fuse, '__version__'):
    raise RuntimeError, "your fuse-py doesn't know of fuse.__version__, probably it's too old."


fuse.fuse_python_api = (0, 2)
fuse.feature_assert('has_init')


class openFile:
        def __init__(self, owner, fd=-1, size=0, file=False, path=False, upload=False, uploadName=False, storeDir=False):
		self.owner = owner
		self.fd = int(fd)
		self.size = int(size)
		self.file = file
		self.path = path
		self.upload = upload
		self.uploadName = uploadName
		self.storeDir = storeDir


class userFile:
	def __init__(self, id, filename_fuse, filename, size, location, sub_location, hidden=False):
		self.id = id
		self.filename_fuse = filename_fuse
		self.filename = filename
		self.size = int(size)
		self.location = location
		self.sub_location = sub_location
		self.hidden = hidden


class upLog:
	def __init__(self, logFile):
		self.logFD = open(logFile, 'wb')

	def __del__(self):
		self.logFD.close()

	def debug(self, message):
		if __debug__:
			self.logFD.write('DEBUG: ' + message + "\n")

	def error(self, message):
		self.logFD.write('*** ERROR: ' + message + " ***\n")


class upDB:
	def __init__(self):
		try:
			self.db = MySQLdb.connect(host="",user="",passwd="",db="")

			c = self.db.cursor()
			c.execute("SET NAMES 'latin1'")
			c.close()
		except:
			self.log.error('DB connect error')
			raise


class MyStat(fuse.Stat):
	def __init__(self):
		self.st_mode = 0
		self.st_ino = 0
		self.st_dev = 0
		self.st_nlink = 0
		self.st_uid = 5000
		self.st_gid = 5000
		self.st_size = 0
		self.st_atime = 0
		self.st_mtime = 0
		self.st_ctime = 0



class UP:
	def __init__(self, log):
		self.users = {}
		self.usersTimer = int(time() - 180)
		self.userFiles = {}
		self.userFilesTimer = {}
		self.openFiles = {}
		self.log = log


	def flag2mode(self, flags):
		md = {os.O_RDONLY: 'r', os.O_WRONLY: 'w', os.O_RDWR: 'w+'}
		m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

		if flags | os.O_APPEND:
			m = m.replace('w', 'a', 1)

		return m


	def get_upload_path(self):
		return 2


	def runSystemCommand(self, command):
		if command == 'readRoot':
			self.get_users(True)


	def getFileFromPath(self, path):
		file = {}
		pe = path.split('/')[1:]
		level = len(pe)

		if level == 1:
			raise 'getFileFromPath: invalid path level'
		elif level == 2:
			file['username'] = pe[0]
			file['filename'] = pe[1]
		elif level == 3:
			if pe[1] != '__hidden__':
				raise IOError('getFileFromPath invalid dir in userdir')
			else:
				file['username'] = pe[0]
				file['filename'] = pe[2]
		else:
			raise 'getFileFromPath: invalid path level'

		return file


	def openFile(self, path, flags):
		pe = path.split('/')[1:]
		level = len(pe)

		if level != 2 and level != 3:
			self.log.error('open_file: invalid level ' + level)
			return -errno.ENOENT

		file = self.get_file_info(path)
		fullpath = '/var/upload/' + str(file.sub_location) + '/' + file.location
		try:
			fileFromPath = self.getFileFromPath(path)
			username = fileFromPath['username']
			filename = fileFromPath['filename']

			o_file = os.fdopen(os.open(fullpath, flags), self.flag2mode(flags))
			o_fd = o_file.fileno()
			o_stat = os.fstat(o_fd)
		except:
			self.log.error('open_file: cant open: ' + fullpath)
			raise

		of = openFile(username, o_fd, o_stat.st_size, o_file, path, False, False, False)
		open_files_id = hashlib.md5(path).hexdigest()
		self.openFiles[open_files_id] = of


	def getOpenFileInfo(self, path):
		pe = path.split('/')[1:]
		level = len(pe)
		if level != 2 and level != 3:
			return -1

		username = pe[0]
		filename = pe[1]

		open_files_id = hashlib.md5(path).hexdigest()
		try:
		    file = self.openFiles[open_files_id]
		except KeyError:
		    return -1

		if file != -1:
			return file

		return -1


	def completeUpload(self, open_file):
		if open_file.upload == False:
			return

		oldName = open_file.uploadName

		try:
			file = self.getFileFromPath(open_file.path)

			# get size
			o_stat = os.stat(oldName)

			justName = hashlib.sha1(oldName + str(time())).hexdigest()
			newName = '/var/upload/' + str(open_file.storeDir) + '/' + justName
			os.rename(oldName, newName)
			os.chown(newName, 60, 60)

			delete_num = random.randint(99999, 999999999)
			sub_location = open_file.storeDir
			filename = file['filename']
			size = o_stat.st_size
			user_id = self.get_user_id(open_file.owner)
			hidden = self.is_hidden_file(open_file.path)

			# update size in userFiles
			for _f in self.userFiles[open_file.owner]:
				if _f.filename == filename:
					_f.size = size
					_f.hidden = hidden
					_f.location = justName
					_f.sub_location = sub_location
					break

			# add file to DB
			db = upDB()

			c = db.db.cursor()
			c.execute("INSERT DELAYED INTO up VALUES(NULL, '', %s, NOW(), '0000-00-00 00:00:00', '127.0.0.1', %s, %s, %s, %s, 'application/octet-stream', %s, 0, 0, 7, 0, '', '0000-00-00 00:00:00', '', 0, 0, %s, %s)", (delete_num, justName, sub_location, filename, filename, size, hidden, user_id))
			c.close()

			# update counters
			d = db.db.cursor()
			d.execute("UPDATE LOW_PRIORITY users SET uploads=uploads+1, uploads_size=uploads_size+%s WHERE id=%s LIMIT 1", (size, user_id))
			d.close()

			db.db.close()

			for _f in self.userFiles[open_file.owner]:
				if _f.filename == filename:
					_f.id = self.get_file_id_from_name(open_file.owner, filename)
					break

		except:
			self.log.error('completeUpload error')



	def closeFile(self, path):
		open_file = self.getOpenFileInfo(path)

		if open_file == -1:
			self.log.error('close file empty: ' + path)
			return

		try:
			open_file.file.close()
		except:
			self.log.error('closeFile error')

		open_files_id = hashlib.md5(path).hexdigest()

		# if upload - rename and add to base
		if open_file.upload == True:
			self.completeUpload(open_file)

		del(self.openFiles[open_files_id])



	def renameFile(self, oldPath, newPath):
		pf = oldPath.split('/')[1:]
		if len(pf) != 2 and len(pf) != 3:
			raise 'rename: invalid level from '

		pt = newPath.split('/')[1:]
		if len(pt) != 2 and len(pt) != 3:
			raise 'rename: invalid level to '


		try:
			_f = self.getFileFromPath(oldPath)
			username = _f['username']
			oldFilename = _f['filename']

			_t = self.getFileFromPath(newPath)
			newFilename = _t['filename']
		except:
			raise


		fileID = self.get_file_id_from_name(username, oldFilename)
		if fileID == -1:
			raise 'rename: fileID'


		# rename in userFiles
		for file in self.userFiles[username]:
			if file.id == fileID:
				file.filename = newFilename
				break

		try:
			db = upDB()
			c = db.db.cursor()
			c.execute("UPDATE up SET filename=%s, filename_fuse=%s WHERE id=%s LIMIT 1", (newFilename, newFilename, str(fileID),))
			c.close()
			db.db.close()
		except:
			raise



	def unlinkFile(self, path):
		pf = path.split('/')[1:]
		level = len(pf)
		if level != 2 and level != 3:
			raise 'Unlink: invalid level'

		_f = self.getFileFromPath(path)
		username = _f['username']
		filename = _f['filename']

		file_id = self.get_file_id_from_name(username, filename)
		if file_id == -1:
			raise 'Unlink: invalid file_id'

		try:
			db = upDB()
			c = db.db.cursor()
			c.execute("UPDATE up SET deleted=1, deleted_date=NOW(), deleted_reason=%s WHERE id=%s", ('Deleted owner by FTP', file_id,))
			c.close()
			db.db.close()

			# todo delete from userFiles
			user_files = self.userFiles[username]
			for file in user_files:
				if file.id == file_id:
					user_files.remove(file)
					self.userFiles[username] = user_files
					break
		except:
			raise


	def get_file_id_from_name(self, username, filename):
		file_id = -1

		for file in self.userFiles[username]:
			if file.filename == filename:
				file_id = file.id
				break

		# GET FROM DB
		if file_id == -1:
			user_id = self.get_user_id(username)
			db = upDB()
			c = db.db.cursor()
			c.execute("SELECT id FROM up WHERE user_id=%s AND filename_fuse=%s LIMIT 1", (user_id, filename))
			file = c.fetchone()
			c.close()
			db.db.close()

			if file != None:
				file_id = file[0]

		return file_id


	def get_user_id(self, username):
		if username in self.users:
			return self.users[username]
		else:
			try:
				db = upDB()
				c = db.db.cursor()
				c.execute("SELECT username,id FROM users WHERE username=%s LIMIT 1", (username,))
				user = c.fetchall()
				c.close()
				db.db.close()

				username = user[0]
				user_id = user[1]
				self.users[username] = user_id
				return user_id
			except:
				self.log.error('get_user_id not in users: ' + username)
				raise




	def get_users(self, ignoreCache=False):
		# for 60 sec return cached user lists
		if ignoreCache == False and int(time() - self.usersTimer) < 60:
			return self.users

		try:
			db = upDB()
			c = db.db.cursor()
			c.execute("SELECT username,id FROM users")
			users = c.fetchall()
			c.close()
			db.db.close()
		except:
			self.log.error('get_users DB error')
			raise

		# clear users list
		self.users.clear()
		for user in users:
			username = user[0]
			user_id = user[1]
			self.users[username] = user_id

		# add timer
		self.usersTimer = time()
		return self.users



	def get_user_files(self, username, ignoreCache=False):
		# for 60 sec return cached user lists
		if ignoreCache == False and username in self.userFilesTimer and int(time() - self.userFilesTimer[username]) < 60:
			if username in self.userFiles:
				return self.userFiles[username]

		try:
			user_id = str(self.get_user_id(username))
		except:
			self.log.error('get_user_files error')
			raise

		userFiles = []
		try:
			db = upDB()
			d = db.db.cursor()
			d.execute("SELECT id,filename_fuse,filename,size,location,sub_location,hidden FROM up WHERE user_id=%s AND deleted=0", (user_id,))
			userfiles = d.fetchall()
			d.close()
			db.db.close()
		except:
			self.log.error('get_user_files DB error 2')
			raise

		for file in userfiles:
			uf = userFile(file[0], file[1], file[2], file[3], file[4], file[5], file[6])
			userFiles.append(uf)

		self.userFiles[username] = userFiles
		self.userFilesTimer[username] = time()

		return userFiles


	def get_file_info(self, path):
		try:
			file = self.getFileFromPath(path)
			username = file['username']
			filename = file['filename']
		except:
			raise

		for file in self.userFiles[username]:
			if (file.filename) == filename:
				return file

		raise IOError('File not exists: ' + path)


	def get_dir_listing(self, path):
		self.log.debug('get_dir_listing: ' + path)
		dirListing = ['.', '..']

		# pe[0] = username
		# pe[1] = file in userdir
		pe = path.split('/')[1:]
		level = len(pe)

		if path == '/':
			# get root - user dirs
			try:
				dirents_users = self.get_users()
			except:
				self.log.error('get_dir_listing error ' + path)
				raise

			for ud in dirents_users:
				dirListing.append(ud)
		elif level == 1:
			try:
				self.get_user_files(pe[0])
			except:
				self.log.error('get_dir_listing error get_user_files' + path)
				raise

			for file in self.userFiles[pe[0]]:
				if file.hidden == False:
					dirListing.append(file.filename)

			# add system dir
			dirListing.append('__hidden__')
		elif 2 == level and pe[1] == '__hidden__':
			# show hidden files
			try:
				self.get_user_files(pe[0])
			except:
				self.log.error('get_dir_listing error get_user_files hidden' + path)
				raise

			for file in self.userFiles[pe[0]]:
				if file.hidden == True:
					dirListing.append(file.filename)

		else:
			raise IOError('invalid path: ' +path)

		return dirListing


	def is_user_dir(self, path):
		pe = path.split('/')[1:]
		username = pe[0]

		if username in self.users:
			return True
		return False


	def is_user_file(self, path):
		self.log.debug('is user file: ' + path)
		try:
			f = self.getFileFromPath(path)
			username = f['username']
			filename = f['filename']
		except:
			self.log.error('== is_user_file FALSE ' + path)
			return False

		try:
			for file in self.userFiles[username]:
				if file.filename == filename:
					self.log.debug('is user file TRUE: ' + path)
					return True
					break
		except KeyError:
			return False

		return False


	def is_hidden_file(self, path):
		self.log.debug('is_hidden_file: ' + path)
		pe = path.split('/')[1:]
		if len(pe) != 3:
			return False

		if pe[1] == '__hidden__':
			return True
		else:
			return False


	def createFile(self, path):
		self.log.debug('createFile: ' + path)
		if self.is_user_file(path):
			self.log.debug('createFile: is user file')
			return -errno.ENOSYS

		pe = path.split('/')[1:]
		if len(pe) != 2 and len(pe) != 3:
			self.log.debug('createFile: wrong len')
			return -errno.ENOSYS

		_f = self.getFileFromPath(path)
		username = _f['username']
		filename = _f['filename']

		userFiles = self.userFiles[username]
		uf = userFile(-1, filename, filename, 0, '0', '0', 0)
		userFiles.append(uf)
		self.userFiles[username] = userFiles

		# open file for writing
		try:
			storeDir = self.get_upload_path()
			fullpath = '/var/upload/' + str(storeDir) + '/tmp_up'
			ofd,filename = mkstemp(suffix='.tmp',dir=fullpath)
			o_file = os.fdopen(ofd, 'w')
			o_fd = o_file.fileno()
		except:
			self.log.error('createFile: cant open new file: ' + filename)
			raise
		else:
			of = openFile(username, o_fd, 0, o_file, path, True, filename, storeDir)
			open_files_id = hashlib.md5(path).hexdigest()
			self.openFiles[open_files_id] = of



	def getAttr(self, path):
		st = MyStat()

		# default uid
		st.st_uid = 5000
		st.st_gid = 5000


		pe = path.split('/')[1:]
		level = len(pe)

		# get user_id
		if level > 1:
			try:
				file = self.getFileFromPath(path)
				fileOwner = file['username']
				user_id = self.users[fileOwner]
				st.st_uid = int(st.st_uid + user_id)
				st.st_gid = int(st.st_gid + user_id)
			except:
				self.log.error('getAttr: ' + path)
				pass


		if path == '/':
			st.st_mode = stat.S_IFDIR | 0700
			st.st_nlink = len(self.users)
		elif 1 == level and self.is_user_dir(path):
			st.st_mode = stat.S_IFDIR | 0700
			st.st_nlink = 1
		elif (2 == level  or 3 == level) and self.is_user_file(path):
			st.st_mode = stat.S_IFREG | 0600
			st.st_nlink = 1
			file = self.get_file_info(path)
			st.st_size = int(file.size)
		elif 2 == level and pe[1] == '__hidden__':
			st.st_mode = stat.S_IFDIR | 0700
			st.st_nlink = 1
		else:
			self.log.debug('getAttr ENOENT: ' + path)
			return -errno.ENOENT

		return st





class upFS(Fuse):
	def __init__(self, *args, **kw):
		Fuse.__init__(self, *args, **kw)

		self.log = upLog('/tmp/fuse.log')
		self.up = UP(self.log)
		self.up.get_dir_listing('/')


	def chmod(self, path, mode):
		# /__system__/command
		pe = path.split('/')[1:]
		level = len(pe)

		if level == 2 and pe[0] == '__system__':
			self.up.runSystemCommand(pe[1])


	def chown(self, path, user, group):
		self.log.debug('chown: '+path)
		pass


	def create(self, path, mode, fi=None):
		try:
			self.up.createFile(path)
		except:
			self.log.error('create: ' + path)


	def flush(self, path):
		open_file = self.up.getOpenFileInfo(path)
		if open_file != -1:
			open_file.file.flush()


	def getattr(self, path):
		try:
			return self.up.getAttr(path)
		except:
			self.log.error('getattr: ' + path)
			return -errno.ENOENT


	def readdir(self, path, offset):
		try:
			dirents = self.up.get_dir_listing(path)
		except:
			self.log.error('readdir invalid path ' + path)
		else:
			for r in dirents:
				yield fuse.Direntry(r)


	def open(self, path, flags):
		try:
			self.up.openFile(path, flags)
		except:
			self.log.error('open: ' + path)
			return -errno.ENOENT


	def write(self, path, buf, offset):
		try:
			open_file = self.up.getOpenFileInfo(path)
			if open_file != -1:
				open_file.file.seek(offset)
				open_file.file.write(buf)
				return len(buf)
		except:
			self.log.error('write: ' + path)
			return 0


	def read(self, path, size, offset):
		try:
			open_file = self.up.getOpenFileInfo(path)
			open_file.file.seek(offset)
			return open_file.file.read(size)
		except:
			self.log.error('read: ' + path)
			return 0


	def mkdir(self, path, mode):
		self.log.debug('mkdir: '+path)
		return -errno.ENOSYS


	def rmdir(self, path):
		return -errno.ENOSYS


	def unlink(self, path):
		self.log.debug('unlink: ' + path)
		try:
			return self.up.unlinkFile(path)
		except:
			self.log.error('unlink: ' + path)
			return -errno.ENOENT


	def mknod(self, path, mode, dev):
		self.log.debug('mknod: '+path)
		return 0


	def access(self, path, mode):
		return 0


	def utime(self, path, times):
		self.log.debug('utime: '+path)
		return 0


	def release(self, path, flags):
		self.log.debug('release: '+path)
		self.up.closeFile(path)


	def truncate(self, path, size):
		self.log.debug('trunk: '+path)
		return 0

	def fsync(self, path, isfsyncfile):
		self.log.debug('fsync: '+path)
		return 0


	def rename(self, pathfrom, pathto):
		try:
			self.up.renameFile(pathfrom, pathto)
		except:
			self.log.error('rename: ' + pathfrom)
			return -errno.ENOENT


	def statfs(self):
		st = fuse.StatVfs()
		st.f_bsize = 512
		# MAYBE TOO SMALL?
		st.f_blocks = 4096
		st.f_favail = 2048
		st.f_namelen = 255
		return st



def main():
	usage="""
Userspace filesystem for project up.iteam.ua.

""" + Fuse.fusage

	try:
		server = upFS(version="%prog " + fuse.__version__, usage=usage, dash_s_do='setsingle')
		server.parse(errex=1)
	except:
		print "FS init error"
		sys.exit(1)

	server.main()

if __name__ == '__main__':
    main()

