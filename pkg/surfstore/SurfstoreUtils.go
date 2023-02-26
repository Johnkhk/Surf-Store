package surfstore

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	fmt.Println("SYNCING", client.BaseDir)
	// 1. Scan base directory
	indexPath := client.BaseDir + "/index.db"
	if _, err := os.Stat(indexPath); errors.Is(err, os.ErrNotExist) {
		os.Create(indexPath)
	}
	var blockAddr string
	err := client.GetBlockStoreAddr(&blockAddr)
	check(err)
	dir, err := ioutil.ReadDir(client.BaseDir)
	check(err)
	// 2. Compute each file's hash list
	curIndex := make(map[string][]string) // map of local files to hashList
	for _, dirFile := range dir {
		if dirFile.Name() != "index.db" {
			curIndex[dirFile.Name()] = getHashList(client, dirFile.Name())
		}
	}
	// get localIndex
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir) // retrieved from index.db
	check(err)
	fmt.Println("Read Local Index")
	PrintMetaMap(localIndex)

	// 4. Client connect to server and download updated FileInfoMap
	remoteIndex := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteIndex)
	check(err)
	fmt.Println("REMOTE: ")
	PrintMetaMap(remoteIndex)

	newlocal := make(map[string]*FileMetaData)

	// case 1
	for serverfname, serverfmeta := range remoteIndex {
		_, inLocalIndex := localIndex[serverfname]
		_, inCurIndex := curIndex[serverfname]
		fmt.Println(serverfname)
		fmt.Println(inLocalIndex, inCurIndex)

		// not in local index
		// if !inLocalIndex {
		// 	localIndex[serverfname] = remoteIndex[serverfname]
		// }
		// if !inCurIndex {
		// 	fmt.Println("downloading")
		// 	downloadFile(client, blockAddr, serverfname, serverfmeta.BlockHashList)
		// }
		// if !inLocalIndex || !inCurIndex {
		if !inLocalIndex {

			downloadFile(client, blockAddr, serverfname, serverfmeta.BlockHashList)
			localIndex[serverfname] = remoteIndex[serverfname]
			newlocal[serverfname] = remoteIndex[serverfname]
			// defer func(serverfname string) { localIndex[serverfname] = remoteIndex[serverfname] }(serverfname)

		}

		if !inCurIndex && inLocalIndex {
			if localIndex[serverfname].Version == serverfmeta.Version && len(localIndex[serverfname].BlockHashList) == 1 && localIndex[serverfname].BlockHashList[0] == "0" {
				fmt.Println("tombstoned, no need to update Version")
				continue
			}
			fmt.Println("File deleted locally, updatin server")
			var newHashList []string
			newHashList = append(newHashList, "0")
			updateFileMetaData := &FileMetaData{
				Filename:      serverfname,
				Version:       localIndex[serverfname].Version + 1,
				BlockHashList: newHashList,
			}
			latestVersion := new(int32)
			err = client.UpdateFile(updateFileMetaData, latestVersion)
			check(err)
			var zero int32 = 0
			// delete unsuccessful
			if *latestVersion < zero {
				fmt.Println("delete unsucc")
				err = client.GetFileInfoMap(&remoteIndex)
				check(err)
				downloadFile(client, blockAddr, serverfname, remoteIndex[serverfname].BlockHashList)
				localIndex[serverfname] = remoteIndex[serverfname]
				// localIndex[serverfname] = remoteIndex[serverfname]

			}
		}

		// if inLocalIndex && inCurIndex {
		// 	fmt.Println("BROO")

		// 	if !Equal(localIndex[serverfname].BlockHashList, curIndex[serverfname]) {
		// 		fmt.Println("MODIFIED")

		// 		dirBlockArr := getBlockArray(client, serverfname)
		// 		// upload blocks to server
		// 		for _, newBlocks := range dirBlockArr {
		// 			var succ bool
		// 			err = client.PutBlock(newBlocks, blockAddr, &succ)
		// 			check(err)
		// 		}
		// 		// update server with new Fileinfo
		// 		Fileinfo := &FileMetaData{
		// 			Filename:      serverfname,
		// 			Version:       serverfmeta.Version + 1,
		// 			BlockHashList: serverfmeta.BlockHashList,
		// 		}
		// 		latestVersion := new(int32)
		// 		err = client.UpdateFile(Fileinfo, latestVersion)
		// 		check(err)
		// 		var zero int32 = 0
		// 		if *latestVersion >= zero { // upload success
		// 			// defer func(serverfname string) { localIndex[serverfname] = Fileinfo }(serverfname)
		// 			newlocal[serverfname] = remoteIndex[serverfname]
		// 		} else {
		// 			fmt.Println("FAILED")
		// 		}
		// 	}
		// }

		// if !inCurIndex {
		// 	fmt.Println("Here -2")
		// 	fmt.Println("downloading remote to local")
		// 	downloadFile(client, blockAddr, serverfname, serverfmeta.BlockHashList)
		// 	fmt.Println("downloading Done")
		// 	localIndex[serverfname] = remoteIndex[serverfname]
		// }
	}

	// case 2
	for curFileName, curHashList := range curIndex {
		_, inLocalIndex := localIndex[curFileName]
		_, inRemoteIndex := remoteIndex[curFileName]
		if !inLocalIndex || !inRemoteIndex {

			fmt.Println("Here -1")
			dirBlockArr := getBlockArray(client, curFileName)
			// upload blocks to server
			for _, newBlocks := range dirBlockArr {
				var succ bool
				err = client.PutBlock(newBlocks, blockAddr, &succ)
				check(err)
			}
			// update server with new Fileinfo
			Fileinfo := &FileMetaData{
				Filename:      curFileName,
				Version:       1,
				BlockHashList: curHashList,
			}
			latestVersion := new(int32)
			err = client.UpdateFile(Fileinfo, latestVersion)
			check(err)
			var zero int32 = 0
			// if *latestVersion >= zero { // upload success
			if *latestVersion > zero { // upload success
				newlocal[curFileName] = Fileinfo
			} else {
				fmt.Println("Here 0", *latestVersion)
				if inRemoteIndex {
					downloadFile(client, blockAddr, curFileName, remoteIndex[curFileName].BlockHashList)
					// defer func(curFileName string) { localIndex[curFileName] = remoteIndex[curFileName] }(curFileName)
					newlocal[curFileName] = remoteIndex[curFileName]

				}

				// version mishap
				// curHashList,

				// case 1 no local modifications
				// if Equal(curHashList, localIndex[curFileName].BlockHashList)

				// case 2 local changes, remote and local same version. then sync to remote. if success sync local

				// case 3 local changes, remote ver > local index ver. download blocks, bring local version up to date

			}
		} else if inLocalIndex && inRemoteIndex { // all have the file
			localChanged := !Equal(curHashList, localIndex[curFileName].BlockHashList)
			if localChanged {
				fmt.Println("MODMODMOD")
			}
			if !localChanged && remoteIndex[curFileName].Version > localIndex[curFileName].Version {
				fmt.Println("Here 1")
				downloadFile(client, blockAddr, curFileName, remoteIndex[curFileName].BlockHashList)
				localIndex[curFileName] = remoteIndex[curFileName]
			} else if localChanged && remoteIndex[curFileName].Version == localIndex[curFileName].Version {
				fmt.Println("Here 2")
				// upload blocks to server
				dirBlockArr := getBlockArray(client, curFileName)
				for _, newBlocks := range dirBlockArr {
					var succ bool
					err = client.PutBlock(newBlocks, blockAddr, &succ)
					check(err)
				}
				// update server with new Fileinfo
				Fileinfo := &FileMetaData{
					Filename:      curFileName,
					Version:       remoteIndex[curFileName].Version + 1,
					BlockHashList: curHashList,
				}
				latestVersion := new(int32)
				err = client.UpdateFile(Fileinfo, latestVersion)
				check(err)
				var zero int32 = 0
				if *latestVersion >= zero { // upload success
					// localIndex[curFileName] = Fileinfo
					fmt.Println("upload success")
					newlocal[curFileName] = Fileinfo
				} else {
					fmt.Println("upload failed")
				}
			} else if localChanged && remoteIndex[curFileName].Version > localIndex[curFileName].Version {
				err = client.GetFileInfoMap(&remoteIndex)
				check(err)
				downloadFile(client, blockAddr, curFileName, remoteIndex[curFileName].BlockHashList)
				newlocal[curFileName] = remoteIndex[curFileName]
			}
		}
	}

	err = client.GetFileInfoMap(&localIndex)
	check(err)
	for a, b := range newlocal {
		localIndex[a] = b
	}
	fmt.Println("writing the following to index.db")
	PrintMetaMap(localIndex)

	fmt.Println(client.BaseDir)
	err = WriteMetaFile(localIndex, client.BaseDir)
	check(err)
	fmt.Println("After write check")
	localIndexz, err := LoadMetaFromMetaFile(client.BaseDir) // retrieved from index.db
	PrintMetaMap(localIndexz)

	// err = client.GetFileInfoMap(&localIndex)
	// check(err)
	// PrintMetaMap(localIndex)

}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
func getHashList(client RPCClient, fileName string) []string {
	var bHashList []string
	filePath := ConcatPath(client.BaseDir, fileName)
	f, err := os.Open(filePath)
	check(err)
	for {
		buffer := make([]byte, client.BlockSize)
		n, err := f.Read(buffer) //readFull?
		if err != nil {
			if err == io.EOF {
				bHashList = append(bHashList, GetBlockHashString(buffer[:n]))
				break
			} else {
				check(err)
			}
		} else {
			bHashList = append(bHashList, GetBlockHashString(buffer[:n]))
		}
	}
	return bHashList
}

func downloadFile(client RPCClient, blockAdd string, fileName string, fileHash []string) {
	//fmt.Println(fileName)
	filePath := ConcatPath(client.BaseDir, fileName)
	_, err := os.Create(filePath)
	check(err)
	err = os.Remove(filePath)
	check(err)
	file, err := os.Create(filePath)
	check(err)
	if len(fileHash) == 1 && fileHash[0] == "0" {
		err := os.Remove(filePath)
		check(err)
	} else {
		fileByte := &Block{}
		for _, h := range fileHash {
			err := client.GetBlock(h, blockAdd, fileByte)
			check(err)
			_, err = file.Write(fileByte.BlockData)
			check(err)
		}
	}
	// fmt.Println("DONEZO")
}

// gets block arrays from basedir file
func getBlockArray(client RPCClient, fileName string) []*Block {
	var blockArr []*Block
	filePath := ConcatPath(client.BaseDir, fileName)
	f, err := os.Open(filePath)
	check(err)
	for {
		buffer := make([]byte, client.BlockSize)
		var curBlock Block
		n, err := f.Read(buffer)
		if err != nil {
			if err == io.EOF {
				curBlock.BlockData = buffer[:n]
				curBlock.BlockSize = int32(n)
				blockArr = append(blockArr, &curBlock)
				break
			} else {
				check(err)
			}
		} else {
			curBlock.BlockData = buffer[:n]
			curBlock.BlockSize = int32(n)
			blockArr = append(blockArr, &curBlock)
		}
	}
	return blockArr

}

// equal function compare two array
func Equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
