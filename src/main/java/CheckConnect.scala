import org.apache.spark.sql.SparkSession

import java.io.DataOutputStream
import java.net.Socket
import java.util.Date

object CheckConnect {

    def send(serverName: String, port: Int, msg: String): Unit = {
        System.out.println("连接到主机：" + serverName + " ，端口号：" + port)
        val client = new Socket(serverName, port)
        System.out.println("远程主机地址：" + client.getRemoteSocketAddress)
        val outToServer = client.getOutputStream
        val out = new DataOutputStream(outToServer)
        out.writeUTF(new Date() + " Hello from " + client.getLocalSocketAddress + " " + msg + "\n")
        //        val inFromServer = client.getInputStream
        //        val in = new DataInputStream(inFromServer)
        //        System.out.println("服务器响应： " + in.readUTF)
        client.close()
    }

    def main(args: Array[String]): Unit = {
        val a = args(0)
        //        val a = "172.18.20.23:9999"
        val Array(ip, port) = a.split(":")

        //        send("172.18.20.23", 9999, "nihao")
        val spark = SparkSession.builder()
            .getOrCreate()
        val sc = spark.sparkContext
        sc.makeRDD(1 until 10)
            .repartition(3)
            .foreachPartition {
                a => {
                    send(ip, port.toInt, a.toList.toString())
                }
            }
    }
}
