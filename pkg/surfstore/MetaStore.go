package surfstore

import (
	context "context"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
	// mutex
	mtx sync.Mutex
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	// check whether the file is in the FileMetaMap
	fName := fileMetaData.Filename
	fVersion := fileMetaData.Version
	if _, ok := m.FileMetaMap[fName]; ok {
		if fVersion == m.FileMetaMap[fName].Version+1 {
			// if fVersion+1 == m.FileMetaMap[fName].Version {
			m.FileMetaMap[fName] = fileMetaData
		} else {
			// fmt.Println(fVersion, m.FileMetaMap[fName].Version)
			// failed to update
			fVersion = -1
		}
	} else {
		// not in the map, just add it
		m.FileMetaMap[fName] = fileMetaData
	}
	return &Version{Version: fVersion}, nil

}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
