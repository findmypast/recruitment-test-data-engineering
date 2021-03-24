import Foundation
import SwiftKueryORM
import SwiftKueryMySQL

Database.default = Database(MySQLConnection.createPool(
  host: "database",
  user: "codetest",
  password: "swordfish",
  database: "codetest",
  poolOptions: ConnectionPoolOptions.init(initialCapacity: 1, maxCapacity: 8)))

struct Example : Model {
  static var tableName = "examples"
  var name: String
}

let semaphore = DispatchSemaphore(value: 0)

for name in try! String(contentsOf: URL(fileURLWithPath: "/data/example.csv"))
                  .trimmingCharacters(in: .whitespacesAndNewlines)
                  .components(separatedBy: "\n").dropFirst(1) {
  Example(name: name).save { _, _ in semaphore.signal()}
  semaphore.wait()
}

Example.findAll { (result: [(Int, Example)]?, error: RequestError?) in
  guard result != nil else {
    print(error!)
    semaphore.signal()
    return
  }

  if let rows = result {
    let text = rows.sorted {$0.0 < $1.0}
                   .map { item in return "{\"id\":\(item.0),\"name\":\"\(item.1.name)\"}"}
                   .joined(separator: ",")

    try! "[\(text)]".write(to: URL(fileURLWithPath: "/data/example_swift.json"), atomically: true, encoding: .utf8)
  }

  semaphore.signal()
}
semaphore.wait()
