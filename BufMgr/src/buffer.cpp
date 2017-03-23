/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/hash_already_present_exception.h"
#include "exceptions/hash_table_exception.h"


namespace badgerdb
{

    BufMgr::BufMgr(std::uint32_t bufs) : numBufs(bufs)
    {
        bufDescTable = new BufDesc[bufs];
        for (FrameId i = 0; i < bufs; i++)
        {
            bufDescTable[i].frameNo = i;
            bufDescTable[i].valid = false;
        }

        bufPool = new Page[bufs];

        int htsize = ((((int) (bufs * 1.2)) * 2) / 2) + 1;
        hashTable = new BufHashTbl(htsize);  // allocate the buffer hash table

        clockHand = bufs - 1;
    }


    BufMgr::~BufMgr()
    {
        for (std::uint32_t i = 0; i < numBufs; i++)
        {
            if (bufDescTable[i].valid && bufDescTable[i].dirty)
            {
                // Flushing all dirty pages to file

                bufDescTable[i].file->writePage(bufPool[i]);
                bufDescTable[i].dirty = false;
            }
        }

        // Deallocating the buffer pool, buffer description table
        // and the hash table
        delete[] bufPool;
        delete[] bufDescTable;
        delete hashTable;
    }

    void BufMgr::advanceClock()
    {
        clockHand = (clockHand + 1) % numBufs;
    }

    /**
     * @brief Allocates a frame for a page using the clock algorithm
     * @param frame The frame number of the buffer pool
     * @return The frame number by reference using the "frame" parameter
     * @throws BufferExceededException If all the pages in all the frames are pinned.
     */
    void BufMgr::allocBuf(FrameId &frame)
    {
        std::uint32_t numPinnedPages = 0;
        for (;;)
        {
            // Advancing clock pointer to the next frame
            advanceClock();

            // Checking if the page is valid
            if (bufDescTable[clockHand].valid)
            {
                // Page is valid
                // Checking if refbit is set
                if (!bufDescTable[clockHand].refbit)
                {
                    // Refbit is not set
                    // Checking if pincount is 0 (i.e. the page
                    // is not used by any user)
                    if (bufDescTable[clockHand].pinCnt == 0)
                    {
                        // Page is not pinned, i.e. it is not
                        // used by any user

                        // Checking if dirty bit is set
                        if (bufDescTable[clockHand].dirty)
                        {
                            // Page is dirty. Flush page to disk
                            bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
                            try
                            {
                                hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
                            }
                            catch (HashNotFoundException hnfe)
                            {
                                std::cerr << hnfe.message() << std::endl;
                                return;
                            }
                            catch (HashTableException hte)
                            {
                                std::cerr << hte.message() << std::endl;
                                return;
                            }
                        }

                        // Clearing the frame (i.e. pinCnt = 0, dirty = false;
                        // valid = false and refbit = false
                        bufDescTable[clockHand].Clear();
                        // Use the frame. Returning frame by reference
                        frame = bufDescTable[clockHand].frameNo;
                        return;
                    }
                    else if (bufDescTable[clockHand].pinCnt > 0)
                    {
                        // Page is pinned, i.e it is used,then
                        // it can't be used. Skip to next frame
                        numPinnedPages++;
                        if (numPinnedPages == numBufs)
                        {
                            throw BufferExceededException();
                        }
                        else
                        {
                            continue;
                        }
                    }
                }
                else
                {
                    // Setting the refbit to false if set
                    bufDescTable[clockHand].refbit = false;
                    continue;
                }
            }
            else
            {
                // Page is not valid
                // Setting up the frame (i.e. pinCnt = 1, dirty = false;
                // valid = true and refbit = true
                bufDescTable[clockHand].Set(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);

                // Use the frame. Returning frame by reference
                frame = bufDescTable[clockHand].frameNo;
                return;
            }
        }
    }

    void BufMgr::readPage(File *file, const PageId pageNo, Page *&page)
    {
        // Checking if file is valid
        if (file == NULL)
        {
            return;
        }

        FrameId frameNo;

        //page = new Page;
        // Checking if the page is present in Hashtable
        // And storing the frame number of the page in frameId by reference
        bool isPagePresentInHashTable;
        try
        {
            isPagePresentInHashTable = hashTable->lookup(file, pageNo, frameNo);
        }
        catch (HashNotFoundException hnfe)
        {
            std::cerr << hnfe.message() << std::endl;
            return;
        }
        catch (HashTableException hte)
        {
            std::cerr << hte.message() << std::endl;
            return;
        }
        // If page is not present in buffer pool
        if (!isPagePresentInHashTable)
        {
            // Allocating a buffer for the page
            try
            {
                allocBuf(frameNo);
            }
            catch (BufferExceededException bee)
            {
                std::cerr << bee.message() << std::endl;
                return;
            }

            //Reading the page
            bufPool[frameNo] = file->readPage(pageNo);

            // Inserting the read page into hashtable
            try
            {
                hashTable->insert(file, pageNo, frameNo);
            }
            catch (HashAlreadyPresentException hape)
            {
                std::cerr << hape.message() << std::endl;
                return;
            }
            catch (HashTableException hte)
            {
                std::cerr << hte.message() << std::endl;
                return;
            }

            // Setting up the frame (i.e. pinCnt = 1, dirty = false;
            // valid = true and refbit = true
            bufDescTable[frameNo].Set(file, pageNo);

            // Assigning the frame to the page, i.e. page points to the frame
            // and returning page by reference
            page = &bufPool[frameNo];
        }
        else // Page is present in the buffer pool
        {
            // Setting the refbit to true
            bufDescTable[frameNo].refbit = true;

            //Incrementing the pin count of the page
            bufDescTable[frameNo].pinCnt++;

            // Assigning the frame to the page, i.e. page points to the frame
            // and returning page by reference
            page = &bufPool[frameNo];
        }
    }

    void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
    {
        if (file == NULL)
        {
            return;
        }
        FrameId frameNo;

        // Checking if the page is present in hash table
        // and also populating frameNo by reference
        try
        {
            if (hashTable->lookup(file, pageNo, frameNo))
            {
                if (bufDescTable[frameNo].pinCnt > 0)
                {
                    // Decrement the pin count
                    bufDescTable[frameNo].pinCnt--;

                    // Set dirty bit if dirty is true
                    if (dirty)
                    {
                        bufDescTable[frameNo].dirty = true;
                    }
                }
                else if (bufDescTable[frameNo].pinCnt == 0)
                {
                    // Pin count is already 0
                    // Throw exception
                    throw PageNotPinnedException(file->filename(), pageNo, frameNo);
                }
            }
        }
        catch (HashNotFoundException hnfe)
        {
            std::cerr << hnfe.message() << std::endl;
            return;
        }
    }

    void BufMgr::flushFile(const File *file)
    {
        if (file == NULL)
        {
            return;
        }

        for (std::uint32_t i = 0; i < numBufs; i++)
        {
            // If page is valid is present in the given file
            if (bufDescTable[i].file == file)
            {
                if (bufDescTable[i].valid)
                {
                    // If page to be flushed is pinned,
                    // throw exception
                    if (bufDescTable[i].pinCnt > 0)
                    {
                        throw PagePinnedException(file->filename(),
                                                  bufDescTable[i].pageNo,
                                                  bufDescTable[i].frameNo);
                    }

                    // If page is dirty, flush it to file and
                    // set dirty bit to false
                    if (bufDescTable[i].dirty)
                    {
                        bufDescTable[i].file->writePage(bufPool[i]);
                        bufDescTable[i].dirty = false;
                    }

                    // Removing the page's entry from hashtable
                    try
                    {
                        hashTable->remove(file, bufDescTable[i].pageNo);
                    }
                    catch (HashNotFoundException hnfe)
                    {
                        std::cerr << hnfe.message() << std::endl;
                        return;
                    }
                    catch (HashTableException hte)
                    {
                        std::cerr << hte.message() << std::endl;
                        return;
                    }
                    // Clearing the frame
                    bufDescTable[i].Clear();
                }
                else
                {
                    // Page is not valid
                    // Throw exception
                    throw BadBufferException(bufDescTable[i].frameNo,
                                             bufDescTable[i].dirty,
                                             bufDescTable[i].valid,
                                             bufDescTable[i].refbit);
                }
            }
        }
    }

    void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
    {
        // Checking if file is valid
        if (file == NULL)
        {
            return;
        }

        FrameId frameNo;

        // Creating a new page in the file
        Page new_page = file->allocatePage();

        // Returning pageNo by reference
        pageNo = new_page.page_number();

        // Allocating a buffer for the page in the buffer pool
        allocBuf(frameNo);
        bufPool[frameNo] = new_page;

        // Entering the (file, PageNo) key into the hashtable
        try
        {
            hashTable->insert(file, pageNo, frameNo);
        }
        catch (HashAlreadyPresentException hape)
        {
            std::cerr << hape.message() << std::endl;
            return;
        }
        catch (HashTableException hte)
        {
            std::cerr << hte.message() << std::endl;
            return;
        }

        // Setting up the frame (i.e. pinCnt = 1, dirty = false;
        // valid = true and refbit = true
        bufDescTable[frameNo].Set(file, pageNo);

        // Assigning the frame to the page, i.e. page points to the frame
        // and returning page by reference
        page = &bufPool[frameNo];
    }

    void BufMgr::disposePage(File *file, const PageId PageNo)
    {
        if (file == NULL)
        {
            return;
        }

        FrameId frameNo;
        // Checking if the page to be disposed is present in
        // the buffer pool
        try
        {
            if (hashTable->lookup(file, PageNo, frameNo))
            {
                // Removing the page's details from bufDescTable
                bufDescTable[frameNo].Clear();

                // Removing the page's entry from hash table
                try
                {
                    hashTable->remove(file, PageNo);
                }
                catch (HashNotFoundException hnfe)
                {
                    std::cerr << hnfe.message() << std::endl;
                    return;
                }
                catch (HashTableException hte)
                {
                    std::cerr << hte.message() << std::endl;
                    return;
                }
            }
        }
        catch (HashNotFoundException hnfe)
        {
            std::cerr << hnfe.message() << std::endl;
            return;
        }

        // Deleting the page from the file
        file->deletePage(PageNo);
    }

    void BufMgr::printSelf(void)
    {
        BufDesc *tmpbuf;
        int validFrames = 0;

        for (std::uint32_t i = 0; i < numBufs; i++)
        {
            tmpbuf = &(bufDescTable[i]);
            std::cout << "FrameNo:" << i << " ";
            tmpbuf->Print();

            if (tmpbuf->valid == true)
                validFrames++;
        }

        std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
    }
}