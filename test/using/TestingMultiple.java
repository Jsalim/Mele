package using;

import com.nearinfinity.mele.Mele;
import com.nearinfinity.mele.store.MeleConfiguration;

public class TestingMultiple {

	public static void main(String[] args) {
		MeleConfiguration configuration = new MeleConfiguration();
		configuration.setRsyncBaseDir(args[0]);
		Mele.getMele(configuration);

	}

}
