package maming.netcat;

/**
 * 1.测试内部类是可以使用外部类的属性的
 * 2.如果内部类是static静态的,则是不能使用外部类的属性的,因此需要外部类传递信息给静态AcceptHandler类
 */
public class TestInnerClass {

	private String hostName;
	private int port;

	public TestInnerClass() {
		this.hostName = "http://www.baidu.com";
		this.port = 999;
	
		/*
		 * AcceptHandler acceptRunnable = new AcceptHandler();
		 * acceptRunnable.run();
		 */
	
		AcceptHandler acceptRunnable = new AcceptHandler();
		acceptRunnable.hostName = hostName;
		acceptRunnable.port = port;
		acceptRunnable.run();
	}

	private static class AcceptHandler {

		private String hostName;
		private int port;

		public void run() {
			System.out.println(toString());
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("hostss:" + hostName).append(",")
					.append("portss:" + port);
			return sb.toString();
		}

	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("host:" + hostName).append(",").append("port:" + port);
		return sb.toString();
	}

	public static void main(String[] args) {
		TestInnerClass test = new TestInnerClass();
		System.out.println(test.toString());
	}

}
