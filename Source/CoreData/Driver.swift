// Driver.swift
//
// Copyright (c) 2014 Shintaro Kaneko (http://kaneshinth.com)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

/**
*  http://www.cocoanetics.com/2012/07/multi-context-coredata/
*/


import Foundation
import CoreData


class Driver: NSObject {
    
    struct Static {
        static let maxConcurrentOperationCount = 1
        static var driverOperationQueue: DriverOperationQueue?
    }

    var coreDataStack : CoreDataStack

    var driverOperationQueue: DriverOperationQueue {
        if let queue = Static.driverOperationQueue {
            return queue
        }
        
        let queue = DriverOperationQueue()
        queue.maxConcurrentOperationCount = Static.maxConcurrentOperationCount
        Static.driverOperationQueue = queue
        return queue
    }
    
    init(coreDataStack: CoreDataStack) {
        self.coreDataStack = coreDataStack
    }
    
    func tearDown(tearDownCoreDataStack: (CoreDataStack) -> Void) {
        Static.driverOperationQueue = nil
        tearDownCoreDataStack(self.coreDataStack)
    }
    
    // MARK: - CRUD
    
    /**
    Create Entity
    
    :param: entityName
    :param: context
    
    :returns:
    */
    func create(entityName: String, context: NSManagedObjectContext?) -> NSManagedObject? {
        if let context = context {
            return NSEntityDescription.insertNewObjectForEntityForName(entityName, inManagedObjectContext: context)
        } else {
            return nil
        }
    }

    /**
    Read Entity
    
    :param: entityName
    :param: predicate
    :param: sortDescriptor
    :param: context
    
    :returns: array of managed objects. nil if an error occurred.
    */
    func read(entityName: String, predicate: NSPredicate? = nil, sortDescriptors: [NSSortDescriptor]? = nil, offset: Int? = 0, limit: Int? = 0, context: NSManagedObjectContext?) throws ->  [NSManagedObject]? {
        if let context = context {

            var request = NSFetchRequest(entityName: entityName)
            if let predicate = predicate {
                request.predicate = predicate
            }
            
            if let sortDescriptors = sortDescriptors {
                request.sortDescriptors = sortDescriptors
            }
            if let offset = offset {
                if offset > 0 {
                    request.fetchOffset = offset
                }
            }
            if let limit = limit {
                if limit > 0 {
                    request.fetchLimit = limit
                }
            }
            
            do {
                return try context.executeFetchRequest(request) as? [NSManagedObject]
            } catch {
                throw error
            }
        } else {
            return nil
        }
    }
    
    /**
    Read Entity with fetchRequest
    
    :param: fetchRequest
    :param: context
    :param: error
    
    :returns: array of managed objects. nil if an error occurred.
    */
    func read(fetchRequest: NSFetchRequest, context: NSManagedObjectContext? = nil) throws -> [NSManagedObject]? {
        
        if let ctx = context ?? self.context() {
            do {
                return try ctx.executeFetchRequest(fetchRequest) as? [NSManagedObject]
            } catch {
                throw error
            }
        }
        return nil
    }
    
    /**
    Count Entities
    
    :param: entityName
    :param: predicate
    :param: context
    
    :returns:
    */
    func count(entityName: String, predicate: NSPredicate? = nil, context: NSManagedObjectContext?, error: NSErrorPointer) -> Int {
        if let context = context {
            let request = NSFetchRequest(entityName: entityName)
            if predicate != nil {
                request.predicate = predicate
            }
            return context.countForFetchRequest(request, error: error)
        } else {
            return 0
        }
    }
    
    /**
    Save for PSC
    
    :param: context
    */
    
    /**
    Recursively save parent contexts
    
    :param: context Context to retrieve parents from.
    :param: error
    */
    private func recursiveSave(context: NSManagedObjectContext?, error: NSErrorPointer) {
        if let parentContext = context?.parentContext {
            parentContext.performBlock({ () -> Void in
                try! parentContext.save()
                if parentContext == self.coreDataStack.writerManagedObjectContext {
                    arprint("Data stored")
                } else if parentContext == self.coreDataStack.defaultManagedObjectContext {
                    arprint("MainQueueContext saved")
                } else {
                    arprint("Recursive save \(parentContext)")
                    }
                    
                    self.recursiveSave(parentContext, error: error)
            })
        }
    }
    
    /**
    Save context and recursively save all parent contexts
    
    :param: context
    :param: error
    
    :returns: true if success
    */
    func save(context: NSManagedObjectContext?, error: NSErrorPointer) -> Bool {
        if error == nil {
            var err: NSError? = nil
            return self.save(context, error: &err)
        }
                
        if let context = context {
            if context.hasChanges {
                context.performBlockAndWait({ () -> Void in
                    do {
                        try context.save()
                        self.recursiveSave(context, error: error)
                    }
                    catch let saveError {
                        error.memory = saveError as NSError
                    }
                })
                if error.memory != nil {
                    arprint("Save failed : \(error.memory?.localizedDescription)")
                    return false
                } else {
                    arprint("Save Success")
                    return true
                }
            } else {
                arprint("Save Success (No changes)")
                return true
            }
        } else {
            arprint("Save failed : context is nil")
            return false
        }
    }

    /**
    Delete a managed object
    
    :param: object managed object
    */
    func delete(object object: NSManagedObject?) {
        if let object = object {
            if let context = object.managedObjectContext {
                context.deleteObject(object)
            }
        }
    }
    
    /**
    Delete all managed objects using predicate
    
    :param: entityName
    :param: predicate
    :param: context
    :param: error

    :returns: true if success
    */
    func delete(entityName entityName: String, predicate: NSPredicate? = nil, context: NSManagedObjectContext? = nil) throws {

        do {
            if let objects = try read(entityName, predicate: predicate, context: context) {
                for object: NSManagedObject in objects {
                    delete(object: object)
                }
            }
        } catch {
            throw error
        }
    }
    
    /**
    Peform block in background queue and save
    
    :param: block
    :param: saveSuccess
    :param: saveFailure
    :param: waitUntilFinished
    */
    func saveWithBlock(block block: (() -> Void)?, saveSuccess: (() -> Void)?, saveFailure: ((error: NSError?) -> Void)?) {
        self.saveWithBlockWaitSave(block: { (save) -> Void in
            block?()
            save()
            }, saveSuccess: saveSuccess, saveFailure: saveFailure, waitUntilFinished: false)
    }
    
    func saveWithBlock(block block: (() -> Void)?, saveSuccess: (() -> Void)?, saveFailure: ((error: NSError?) -> Void)?, waitUntilFinished:Bool) {
        self.saveWithBlockWaitSave(block: { (save) -> Void in
            block?()
            save()
            }, saveSuccess: saveSuccess, saveFailure: saveFailure, waitUntilFinished: waitUntilFinished)
    }
    
    /**
    Perform block in background queue and save and wait till done.
    
    :param: block
    :param: error error pointer
    
    :returns: true if success
    */
    func saveWithBlockAndWait(block block: (() -> Void)?, error: NSErrorPointer) -> Bool {
        var result: Bool = true
        var _error = error
        self.saveWithBlock(block: block, saveSuccess: { () -> Void in
        }, saveFailure: { (error) -> Void in
            result = false
            _error.memory = error
        }, waitUntilFinished: true)
        return result
    }
    
    /**
    Perform in background queue and save (Manually call timing of save.)
    
    :param: block             Block to perform. Call save() to invoke save.
    :param: saveSuccess
    :param: saveFailure
    :param: waitUntilFinished
    */
    func saveWithBlockWaitSave(block block: ((save: (() -> Void)) -> Void)?, saveSuccess: (() -> Void)?, saveFailure: ((error: NSError?) -> Void)?) {
        self.saveWithBlockWaitSave(block: block, saveSuccess: saveSuccess, saveFailure: saveFailure, waitUntilFinished: false)
    }
    
    func saveWithBlockWaitSave(block block: ((save: (() -> Void)) -> Void)?, saveSuccess: (() -> Void)?, saveFailure: ((error: NSError?) -> Void)?, waitUntilFinished: Bool) {
        if let block = block {

            if let context = self.coreDataStack.defaultManagedObjectContext {
                let operation = DriverOperation(parentContext: context) { (localContext) -> Void in
                    block(save: { () -> Void in
                        var error: NSError? = nil
                        do {
                            try localContext.obtainPermanentIDsForObjects(Array(localContext.insertedObjects))
                            if error != nil {
                                dispatch_sync(dispatch_get_main_queue(), { () -> Void in
                                    saveFailure?(error: error)
                                    return
                                })
                            } else {
                                if self.save(localContext, error: &error) {
                                    dispatch_sync(dispatch_get_main_queue(), { () -> Void in
                                        saveSuccess?()
                                        return
                                    })
                                } else {
                                    dispatch_async(dispatch_get_main_queue(), { () -> Void in
                                        saveFailure?(error: error)
                                        return
                                    })
                                }
                            }
                            
                        } catch {
                            dispatch_async(dispatch_get_main_queue(), { () -> Void in
                                saveFailure?(error: error as NSError)
                                return
                            })
                        }
                    })
                    return
                }
                
                if waitUntilFinished {
                    self.driverOperationQueue.addOperations([operation], waitUntilFinished: true)
                } else {
                    self.driverOperationQueue.addOperation(operation)
                }
                
            }
        }
    }
    
    /**
    Peform block in background queue and save
    
    :param: block
    :param: waitUntilFinished
    */
    func performBlock(block block: (() -> Void)?, completion: (() -> Void)?) {
        self.performBlock(block: block, completion: completion, waitUntilFinished: false)
    }
    
    func performBlock(block block: (() -> Void)?, completion: (() -> Void)?, waitUntilFinished: Bool) {
        if let block = block {
            if let context = self.coreDataStack.defaultManagedObjectContext {
                let operation = DriverOperation(parentContext: context) { (localContext) -> Void in
                    block()
                    if let completion = completion {
                        dispatch_sync(dispatch_get_main_queue(), { () -> Void in
                            completion()
                        })
                    }
                    return
                }
                
                if waitUntilFinished {
                    self.driverOperationQueue.addOperations([operation], waitUntilFinished: true)
                } else {
                    self.driverOperationQueue.addOperation(operation)
                }
            }
        }
    }
    
    /**
    
    Returns a NSManagedObjectContext associated to currennt operation queue.
    Operation queues should be a Main Queue or a DriverOperationQueue
    
    :returns: A managed object context associated to current operation queue.
    */
    func context() -> NSManagedObjectContext? {
        if let queue = NSOperationQueue.currentQueue() {
            if queue == NSOperationQueue.mainQueue() {
                return self.coreDataStack.defaultManagedObjectContext
            } else if queue.isKindOfClass(DriverOperationQueue) {
                return Static.driverOperationQueue?.currentExecutingOperation?.context
            }
        }
        
        // temporarily use "context for current thread"
        // context associated to thread
        if NSThread.isMainThread() {
            return self.coreDataStack.defaultManagedObjectContext
        } else {
            let kNSManagedObjectContextThreadKey = "kNSManagedObjectContextThreadKey"
            let threadDictionary = NSThread.currentThread().threadDictionary
            if let context = threadDictionary[kNSManagedObjectContextThreadKey] as? NSManagedObjectContext {
                return context
            } else {
                let context = NSManagedObjectContext(concurrencyType: NSManagedObjectContextConcurrencyType.PrivateQueueConcurrencyType)
                context.parentContext = self.coreDataStack.defaultManagedObjectContext
                context.mergePolicy = NSOverwriteMergePolicy
                threadDictionary.setObject(context, forKey: kNSManagedObjectContextThreadKey)
                return context
            }
        }
        

// temporarily comment out assert
//        assert(false, "Managed object context not found. Managed object contexts should be created in an DriverOperationQueue.")
//        return nil
    }
    
    /**
    
    Returns the default Managed Object Context for use in Main Thread.
    
    :returns: The default Managed Object Context
    */
    func mainContext() -> NSManagedObjectContext? {
        return self.coreDataStack.defaultManagedObjectContext
    }
    
    
    /**
    Check if migration is needed.
    
    :returns: true if migration is needed. false if not needed (includes case when persistent store is not found).
    */
    func isRequiredMigration() -> Bool {
        return self.coreDataStack.isRequiredMigration()
    }
}
    

// MARK: - Printable

extension Driver {
    override var description: String {
        let description = "Stored URL: \(self.coreDataStack.storeURL)"
        return description
    }
}

/**
*  Operation Queue to use when performing blocks in background
*/
class DriverOperationQueue: NSOperationQueue {
    
    /**
    Add an Operaion to this Operaion Queue. Operations will run in serial.
    
    :param: op Operation
    */
    override func addOperation(op: NSOperation) {
        arprint("Add Operation")
        if let lastOperation = self.operations.last {
            op.addDependency(lastOperation)
        }
        super.addOperation(op)
    }
    /// Current executing operation. nil if none is executing.
    var currentExecutingOperation: DriverOperation? {
        for operation in self.operations as! [DriverOperation] {
            if operation.executing {
                return operation
            }
        }
        return nil
    }
}

/**
*  Operation to use with DriverOperation
*/
class DriverOperation: NSBlockOperation {

    /// Managed Object Context associated to this Operation
    let context: NSManagedObjectContext = NSManagedObjectContext(concurrencyType: .PrivateQueueConcurrencyType)
    
    /**
    Initialize with parent Managed Object Context. It will be the parent of the context which will be associated to this Operation.
    
    :param: parentContext The parent of the context which will be associated to this Operation.
    
    :returns:
    */
    convenience init(parentContext: NSManagedObjectContext, block: ((localContext: NSManagedObjectContext) -> Void)) {
        
        self.init()
        
        let context = self.context
        context.parentContext = parentContext
        context.mergePolicy = NSOverwriteMergePolicy
        
        self.addExecutionBlock({ () -> Void in
            
            block(localContext: context)
        })
    }
}
