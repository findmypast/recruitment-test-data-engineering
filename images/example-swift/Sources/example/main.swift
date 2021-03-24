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
let file = try? String(contentsOf: URL(fileURLWithPath: "/data/example.csv"))
if let text = file {
  let items = text.trimmingCharacters(in: .whitespacesAndNewlines).components(separatedBy: "\n").dropFirst(1)
  for name in items {
    let row = Example(name: name)
    row.save { example, error in
      semaphore.signal()
    }
    semaphore.wait()
  }
}

Example.findAll { (result: [Int: Example]?, error: RequestError?) in
  if let rows = result {
    let name_rows = rows.mapValues { example in
      return example.name
    }

    do {
        let json_data = try JSONEncoder().encode(name_rows)
        try json_data.write(to: URL(fileURLWithPath: "/data/example_swift.json"))
    } catch {
        print(error)
    }
  }

  semaphore.signal()
}
semaphore.wait()
