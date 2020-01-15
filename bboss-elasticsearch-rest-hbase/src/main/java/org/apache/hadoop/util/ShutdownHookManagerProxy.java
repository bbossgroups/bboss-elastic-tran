
package org.apache.hadoop.util;

public class ShutdownHookManagerProxy   {

    public void destroy() throws Exception {
        ShutdownHookManager.get().getShutdownHooksInOrder();
    }

}
