// swift-tools-version:5.3

import PackageDescription

let package = Package(
    name: "example",
    dependencies: [
        .package(url: "https://github.com/IBM-Swift/Swift-Kuery.git", from: "3.0.1"),
        .package(url: "https://github.com/IBM-Swift/SwiftKueryMySQL.git", from: "2.0.2")
    ],
    targets: [
        .target(
            name: "example",
            dependencies: []),
    ]
)
