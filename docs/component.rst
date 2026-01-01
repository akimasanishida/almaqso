Component
==================

This section provides a detailed overview of the internal structure of the ALMAQSO Library container, breaking it down into its modules and classes.
This section may be updated frequently since the development phase is moving from PoC (Proof of Concept) to a well-structured library.

Purpose
-------

This diagram explains how the ALMAQSO Library is structured internally, showing the main components and their interactions.

Audience
--------

- Developers working on ALMAQSO
- New contributors

Components
----------

Here, we explain the main components of the ALMAQSO Library:

- **Almaqso Class**: The main class that provides the public API for users to interact with the library.
- **Logger Manager**: A component responsible for initializing and configuring logging within the library.
- **Query Class**: A class that handles querying the ALMA Science Archive for calibration source data.
- **Download Function**: A function that manages the downloading of data from the ALMA Science Archive.
- **Process Class**: A class that encapsulates the logic for processing downloaded data using CASA tasks.
- **Analysis Functions or Classes**: Components that provide functionalities for analyzing the processed data. This is not yet implemented.

C4-Component Diagram
--------------------

:numref:`fig-L3-component` shows the component diagram of the ALMAQSO Library.

.. figure:: diagrams/L3-component.svg
   :name: fig-L3-component
   :class: thumbnail
   :target: _static/L3-component.svg
   :align: center
   :alt: C4-Component Diagram
   
   C4-Component Diagram. (Web version: Click to show full-size diagram.)